package ru.yandex.qe.s3.amazon.transfer;

import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.joda.time.Duration;
import ru.yandex.qe.s3.amazon.transfer.loop.AmazonDownloadProcessingLoop;
import ru.yandex.qe.s3.amazon.transfer.loop.AmazonUploadProcessingLoop;
import ru.yandex.qe.s3.transfer.TTLTransmitter;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferSizeType;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadState;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.transfer.factories.BaseTTLTransmitter;
import ru.yandex.qe.s3.transfer.loop.DownloadProcessingLoop;
import ru.yandex.qe.s3.transfer.loop.UploadProcessingLoop;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;
import ru.yandex.qe.s3.ttl.S3Type;
import ru.yandex.qe.s3.ttl.TTLRegister;
import ru.yandex.qe.s3.util.function.ThrowingFunction;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

/**
 * Established by terry on 29.07.15.
 */
public class AmazonTTLTransmitterFactory extends AmazonTransmitterFactory {

    private final TTLRegister ttlRegister;
    private final String s3Endpoint;

    public AmazonTTLTransmitterFactory(@Nonnull AmazonS3 amazonS3, String s3Endpoint,
        @Nonnull TTLRegister ttlRegister) {
        super(ByteBufferSizeType._8_MB, amazonS3);
        this.s3Endpoint = s3Endpoint;
        this.ttlRegister = ttlRegister;
    }

    public AmazonTTLTransmitterFactory(@Nonnull ByteBufferSizeType bufferSizeType, @Nonnull AmazonS3 amazonS3,
        String s3Endpoint, @Nonnull TTLRegister ttlRegister) {
        super(bufferSizeType, amazonS3);
        this.s3Endpoint = s3Endpoint;
        this.ttlRegister = ttlRegister;
    }

    public TTLTransmitter create(@Nonnull Duration defaultTTL, @Nonnull RetryPolicy retryPolicy,
        @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService transferExecutor,
        @Nonnull ListeningExecutorService chunksExecutor, @Nonnull ListeningExecutorService consumeExecutor) {
        return new BaseTTLTransmitter(ttlRegister, defaultTTL, byteBufferPool, transferExecutor, chunksExecutor,
            consumeExecutor) {
            @Nonnull
            @Override
            public String getS3Endpoint() {
                return s3Endpoint;
            }

            @Nonnull
            @Override
            public S3Type geS3Type() {
                return S3Type.AMAZON;
            }

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
                @Nullable Consumer<UploadState> progressListener, @Nullable Executor executor,
                @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService chunksExecutor) {
                return new AmazonUploadProcessingLoop(amazonS3, byteBufferPool, chunksExecutor, uploadRequest,
                    progressListener, executor);
            }
        };
    }

    public TTLTransmitter sameThreadTTLTransmitter(@Nonnull Duration defaultTTL) {
        return create(defaultTTL, new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY,
                PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY, false),
            createByteBufferPool("ttl-transmitter", byteBufferSizeType, 1), newDirectExecutorService(),
            newDirectExecutorService(),
            newDirectExecutorService());
    }

    public TTLTransmitter sameThreadTTLTransmitter(@Nonnull Duration defaultTTL,
        RetryPolicy streamReadFailRetryPolicy) {
        return create(defaultTTL, streamReadFailRetryPolicy,
            createByteBufferPool("ttl-transmitter", byteBufferSizeType, 1),
            newDirectExecutorService(), listeningDecorator(Executors.newSingleThreadExecutor()),
            listeningDecorator(Executors.newSingleThreadExecutor()));
    }

    public TTLTransmitter sameThreadTTLTransmitter(@Nonnull Duration defaultTTL, int streamReadFailRetryCount) {
        return sameThreadTTLTransmitter(defaultTTL,
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY, streamReadFailRetryCount, false));
    }

    public TTLTransmitter fixedPoolsTTLTransmitter(@Nonnull String transmitterName, @Nonnull Duration defaultTTL,
        int downloadsPoolSize, int chunksPoolSize) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(defaultTTL,
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY,
                PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY, false),
            createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "ttl-transmitter", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "ttl-chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "ttl-consumer", poolNumber));
    }

    public TTLTransmitter fixedPoolsTTLTransmitter(@Nonnull String transmitterName, @Nonnull Duration defaultTTL,
        int downloadsPoolSize, int chunksPoolSize, int streamReadFailRetryCount) {
        return fixedPoolsTTLTransmitter(transmitterName, defaultTTL, downloadsPoolSize, chunksPoolSize,
            new RetryPolicy(DEFAULT_RETRY_CONDITION, DEFAULT_BACKOFF_STRATEGY, streamReadFailRetryCount, false));
    }

    public TTLTransmitter fixedPoolsTTLTransmitter(@Nonnull String transmitterName, @Nonnull Duration defaultTTL,
        int downloadsPoolSize, int chunksPoolSize, RetryPolicy streamReadFailRetryPolicy) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(defaultTTL, streamReadFailRetryPolicy,
            createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "ttl-transmitter", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "ttl-chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "ttl-consumer", poolNumber));
    }
}
