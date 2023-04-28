package ru.yandex.qe.s3.transfer.factories;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import ru.yandex.qe.s3.transfer.Transmitter;
import ru.yandex.qe.s3.transfer.buffers.ByteBufferPool;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadResult;
import ru.yandex.qe.s3.transfer.download.DownloadState;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.transfer.loop.DownloadProcessingLoop;
import ru.yandex.qe.s3.transfer.loop.UploadProcessingLoop;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;
import ru.yandex.qe.s3.util.function.ThrowingFunction;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Established by terry on 30.01.16.
 */
public abstract class BaseTransmitter implements Transmitter {

    private final ListeningExecutorService transferExecutor;
    private final ListeningExecutorService chunksExecutor;

    private final ByteBufferPool byteBufferPool;

    private final ListeningExecutorService consumeExecutor;

    public BaseTransmitter(@Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService transferExecutor,
        @Nonnull ListeningExecutorService chunksExecutor,
        @Nonnull ListeningExecutorService consumeExecutor) {
        this.byteBufferPool = byteBufferPool;
        this.transferExecutor = transferExecutor;
        this.chunksExecutor = chunksExecutor;
        this.consumeExecutor = consumeExecutor;
    }

    public abstract <T> DownloadProcessingLoop<T> create(@Nonnull DownloadRequest downloadRequest,
        @Nonnull ThrowingFunction<MetaAndStream, T> processor,
        @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor,
        @Nonnull ByteBufferPool byteBufferPool,
        @Nonnull ListeningExecutorService chunksExecutor,
        @Nonnull ListeningExecutorService consumeExecutor);

    public abstract UploadProcessingLoop create(@Nonnull UploadRequest uploadRequest,
        @Nullable Consumer<UploadState> progressListener, @Nullable Executor notifyExecutor,
        @Nonnull ByteBufferPool byteBufferPool, @Nonnull ListeningExecutorService chunksExecutor);

    public ListenableFuture<UploadState> upload(@Nonnull UploadRequest request) {
        //noinspection ConstantConditions
        return upload(request, null, null);
    }

    @Override
    public ListenableFuture<UploadState> upload(@Nonnull UploadRequest request,
        @Nullable Consumer<UploadState> progressListener, @Nullable Executor notifyExecutor) {
        return transferExecutor.submit(
            create(request, progressListener, notifyExecutor, byteBufferPool, chunksExecutor));
    }

    @Override
    public <T> ListenableFuture<DownloadResult<T>> downloadF(@Nonnull DownloadRequest request,
        @Nonnull ThrowingFunction<MetaAndStream, T> processor) {
        //noinspection ConstantConditions
        return downloadF(request, processor, null, null);
    }

    @Override
    public <T> ListenableFuture<DownloadResult<T>> downloadF(@Nonnull DownloadRequest request,
        @Nonnull ThrowingFunction<MetaAndStream, T> processor,
        @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor) {
        return transferExecutor.submit(
            create(request, processor, progressListener, notifyExecutor, byteBufferPool, chunksExecutor,
                consumeExecutor));
    }

    @Override
    public ListenableFuture<DownloadResult<Void>> downloadC(@Nonnull DownloadRequest request,
        @Nonnull ThrowingConsumer<MetaAndStream> consumer,
        @Nonnull Consumer<DownloadState> progressListener, @Nonnull Executor notifyExecutor) {
        return downloadF(request, metaAndStream -> {
            consumer.accept(metaAndStream);
            return null;
        }, progressListener, notifyExecutor);
    }

    @Override
    public ListenableFuture<DownloadResult<Void>> downloadC(@Nonnull DownloadRequest request,
        @Nonnull ThrowingConsumer<MetaAndStream> consumer) {
        //noinspection ConstantConditions
        return downloadF(request, metaAndStream -> {
            consumer.accept(metaAndStream);
            return null;
        }, null, null);
    }
}
