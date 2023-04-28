package ru.yandex.qe.s3.transfer;

import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nonnull;
import ru.yandex.qe.s3.transfer.ttl.TTLUploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Established by terry on 23.07.15.
 */
public interface TTLTransmitter extends Transmitter {

    ListenableFuture<UploadState> upload(@Nonnull TTLUploadRequest request);

    ListenableFuture<UploadState> upload(@Nonnull TTLUploadRequest request,
        @Nonnull Consumer<UploadState> progressListener, @Nonnull Executor notifyExecutor);
}
