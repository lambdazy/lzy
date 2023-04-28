package ru.yandex.qe.s3.amazon.transfer;

import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import ru.yandex.qe.s3.amazon.policy.LogBackoffStrategy;
import ru.yandex.qe.s3.amazon.policy.LogRetryCondition;
import ru.yandex.qe.s3.amazon.transfer.loop.AmazonDownloadProcessingLoop;
import ru.yandex.qe.s3.amazon.transfer.loop.AmazonUploadProcessingLoop;
import ru.yandex.qe.s3.transfer.DownloadTransmitter;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferSizeType;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadState;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.transfer.factories.BaseTransmitter;
import ru.yandex.qe.s3.transfer.factories.BaseTransmitterFactory;
import ru.yandex.qe.s3.transfer.loop.DownloadProcessingLoop;
import ru.yandex.qe.s3.transfer.loop.UploadProcessingLoop;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;
import ru.yandex.qe.s3.util.function.ThrowingFunction;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

/**
 * Established by terry on 29.07.15.
 */
public class AmazonTransmitterFactory extends BaseTransmitterFactory {

    protected static final RetryPolicy.RetryCondition DEFAULT_RETRY_CONDITION =
        new LogRetryCondition(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION);
    protected static final RetryPolicy.BackoffStrategy DEFAULT_BACKOFF_STRATEGY =
        new LogBackoffStrategy(PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY);

    protected static final AtomicLong POOL_NUMBER = new AtomicLong();

    protected final AmazonS3 amazonS3;

    public AmazonTransmitterFactory(@Nonnull AmazonS3 amazonS3) {
        super(ByteBufferSizeType._8_MB);
        this.amazonS3 = amazonS3;
    }

    public AmazonTransmitterFactory(ByteBufferSizeType byteBufferSizeType, @Nonnull AmazonS3 amazonS3) {
        super(byteBufferSizeType);
        this.amazonS3 = amazonS3;
    }

    @Override
    public Transmitter create(@Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService transferExecutor, @Nonnull ListeningExecutorService chunksExecutor,
        @Nonnull ListeningExecutorService consumeExecutor) {
        return create(new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY,
                PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY, false),
            byteBufferPool, transferExecutor, chunksExecutor, consumeExecutor);
    }

    public Transmitter create(RetryPolicy retryPolicy, @Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService transferExecutor, @Nonnull ListeningExecutorService chunksExecutor,
        @Nonnull ListeningExecutorService consumeExecutor) {
        return new BaseTransmitter(byteBufferPool, transferExecutor, chunksExecutor, consumeExecutor) {
            @Override
            public <T> DownloadProcessingLoop<T> create(@Nonnull DownloadRequest downloadRequest,
                @Nonnull ThrowingFunction<MetaAndStream, T> processor,
                @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor,
                @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService chunksExecutor,
                @Nonnull ListeningExecutorService consumeExecutor) {
                return new AmazonDownloadProcessingLoop<>(amazonS3, retryPolicy, byteBufferPool, chunksExecutor,
                    consumeExecutor, downloadRequest, processor, progressListener, notifyExecutor);
            }

            @Override
            public UploadProcessingLoop create(@Nonnull UploadRequest uploadRequest,
                @Nullable Consumer<UploadState> progressListener, @Nullable Executor notifyExecutor,
                @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService chunksExecutor) {
                return new AmazonUploadProcessingLoop(amazonS3, byteBufferPool, chunksExecutor, uploadRequest,
                    progressListener, notifyExecutor);
            }
        };
    }

    public DownloadTransmitter sameThreadDownloadTransmitter(RetryPolicy streamReadFailRetryPolicy) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(streamReadFailRetryPolicy, createByteBufferPool("download", byteBufferSizeType, 1),
            newDirectExecutorService(),
            //chunk thread should be separated thread, because if consumer exit without reading data,
            // download execution will be blocked on pipe
            listeningDecorator(Executors.newSingleThreadExecutor()),
            createSingleThreadPool("consumer", poolNumber));
    }

    public DownloadTransmitter sameThreadDownloadTransmitter(int streamReadFailRetryCount) {
        return sameThreadDownloadTransmitter(
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY, streamReadFailRetryCount, false));
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize, RetryPolicy streamReadFailRetryPolicy) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(streamReadFailRetryPolicy,
            createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "download", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "consumer", poolNumber));
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize, int streamReadFailRetryCount) {
        return fixedPoolsDownloadTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize,
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY, streamReadFailRetryCount, false));
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize) {
        return fixedPoolsDownloadTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize,
            PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY);
    }

    public Transmitter sameThreadTransmitter(RetryPolicy streamReadFailRetryPolicy) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(streamReadFailRetryPolicy, createByteBufferPool("transmitter", byteBufferSizeType, 1),
            newDirectExecutorService(),
            //chunk thread should be separated thread, because if consumer exit without reading data,
            // download execution will be blocked on pipe
            listeningDecorator(Executors.newSingleThreadExecutor()),
            createSingleThreadPool("consumer", poolNumber));
    }

    public Transmitter sameThreadTransmitter(int streamReadFailRetryCount) {
        return sameThreadTransmitter(
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY, streamReadFailRetryCount, false));
    }

    public Transmitter sameThreadTransmitter() {
        return sameThreadTransmitter(PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY);
    }

    public Transmitter fixedPoolsTransmitter(String transmitterName, int downloadsPoolSize, int chunksPoolSize,
        RetryPolicy streamReadFailRetryPolicy) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(streamReadFailRetryPolicy,
            createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "transmitter", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "consumer", poolNumber));
    }

    public Transmitter fixedPoolsTransmitter(String transmitterName, int downloadsPoolSize, int chunksPoolSize,
        int streamReadFailRetryCount) {
        return fixedPoolsTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize,
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY, streamReadFailRetryCount, false));
    }

    public Transmitter fixedPoolsTransmitter(String transmitterName, int downloadsPoolSize, int chunksPoolSize) {
        return fixedPoolsTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize,
            PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY);
    }
}
