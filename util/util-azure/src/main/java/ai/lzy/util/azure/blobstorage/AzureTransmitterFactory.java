package ai.lzy.util.azure.blobstorage;

import ai.lzy.util.azure.blobstorage.transfer.AzureDownloadProcessingLoop;
import ai.lzy.util.azure.blobstorage.transfer.AzureUploadProcessingLoop;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.DownloadRetryOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
import java.util.function.Consumer;

public class AzureTransmitterFactory extends BaseTransmitterFactory {

    public static final ParallelTransferOptions DEFAULT_PARALLEL_TRANSFER_OPTIONS =
        new ParallelTransferOptions().setMaxConcurrency(1);
    public static final DownloadRetryOptions DEFAULT_RETRY_OPTIONS = new DownloadRetryOptions().setMaxRetryRequests(5);
    private final BlobServiceClient client;

    public AzureTransmitterFactory(BlobServiceClient client, ByteBufferSizeType byteBufferSizeType) {
        super(byteBufferSizeType);
        this.client = client;
    }

    public AzureTransmitterFactory(BlobServiceClient client) {
        super(ByteBufferSizeType._8_MB);
        this.client = client;
    }

    public Transmitter create(DownloadRetryOptions retryOptions, ParallelTransferOptions options,
        @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService transferExecutor,
        @Nonnull ListeningExecutorService chunksExecutor, @Nonnull ListeningExecutorService consumeExecutor) {
        return new BaseTransmitter(byteBufferPool, transferExecutor, chunksExecutor, consumeExecutor) {
            @Override
            public <T> DownloadProcessingLoop<T> create(@Nonnull DownloadRequest downloadRequest,
                @Nonnull ThrowingFunction<MetaAndStream, T> processor,
                @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor,
                @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService chunksExecutor,
                @Nonnull ListeningExecutorService consumeExecutor) {
                return new AzureDownloadProcessingLoop<>(client, retryOptions, byteBufferPool, chunksExecutor,
                    consumeExecutor, downloadRequest, processor, progressListener, notifyExecutor);
            }

            @Override
            public UploadProcessingLoop create(@Nonnull UploadRequest uploadRequest,
                @Nullable Consumer<UploadState> progressListener, @Nullable Executor notifyExecutor,
                @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService chunksExecutor) {
                return new AzureUploadProcessingLoop(client, options, byteBufferPool, chunksExecutor, uploadRequest,
                    progressListener, notifyExecutor);
            }
        };
    }

    @Override
    public Transmitter create(@Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService transferExecutor, @Nonnull ListeningExecutorService chunksExecutor,
        @Nonnull ListeningExecutorService consumeExecutor) {
        return create(DEFAULT_RETRY_OPTIONS, DEFAULT_PARALLEL_TRANSFER_OPTIONS, byteBufferPool, transferExecutor,
            chunksExecutor, consumeExecutor);
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize, DownloadRetryOptions retryOptions, ParallelTransferOptions options) {
        final long poolNumber = POOL_NUMBER.incrementAndGet();
        return create(retryOptions, options,
            createByteBufferPool(transmitterName, byteBufferSizeType, downloadsPoolSize + chunksPoolSize),
            fixedThreadPool(transmitterName, downloadsPoolSize, "download", poolNumber),
            fixedThreadPool(transmitterName, chunksPoolSize, "chunks", poolNumber),
            fixedThreadPool(transmitterName, downloadsPoolSize, "consumer", poolNumber));
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize, int streamReadFailRetryCount) {
        return fixedPoolsDownloadTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize,
            new DownloadRetryOptions().setMaxRetryRequests(streamReadFailRetryCount),
            DEFAULT_PARALLEL_TRANSFER_OPTIONS);
    }

    public DownloadTransmitter fixedPoolsDownloadTransmitter(String transmitterName, int downloadsPoolSize,
        int chunksPoolSize) {
        return fixedPoolsDownloadTransmitter(transmitterName, downloadsPoolSize, chunksPoolSize,
            PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY);
    }
}
