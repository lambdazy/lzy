package ru.yandex.qe.s3.transfer;

import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import ru.yandex.qe.s3.transfer.download.DownloadRequest;
import ru.yandex.qe.s3.transfer.download.DownloadResult;
import ru.yandex.qe.s3.transfer.download.DownloadState;
import ru.yandex.qe.s3.transfer.download.MetaAndStream;
import ru.yandex.qe.s3.util.function.ThrowingConsumer;
import ru.yandex.qe.s3.util.function.ThrowingFunction;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Established by terry on 23.07.15.
 * <p>
 * Consumer should close input stream fully read content. Download task will be terminated with error if consumer close
 * stream before fully read or will finish without closing stream
 */
public interface DownloadTransmitter {

    public <T> ListenableFuture<DownloadResult<T>> downloadF(@Nonnull DownloadRequest request,
        @Nonnull ThrowingFunction<MetaAndStream, T> processor);

    public <T> ListenableFuture<DownloadResult<T>> downloadF(@Nonnull DownloadRequest request,
        @Nonnull ThrowingFunction<MetaAndStream, T> processor,
        @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor);

    public ListenableFuture<DownloadResult<Void>> downloadC(@Nonnull DownloadRequest request,
        @Nonnull ThrowingConsumer<MetaAndStream> consumer);

    public ListenableFuture<DownloadResult<Void>> downloadC(@Nonnull DownloadRequest request,
        @Nonnull ThrowingConsumer<MetaAndStream> consumer,
        @Nullable Consumer<DownloadState> progressListener, @Nullable Executor notifyExecutor);
}
