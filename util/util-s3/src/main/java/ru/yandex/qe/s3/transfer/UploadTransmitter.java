package ru.yandex.qe.s3.transfer;

import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nonnull;
import ru.yandex.qe.s3.transfer.upload.UploadRequest;
import ru.yandex.qe.s3.transfer.upload.UploadState;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Established by terry on 23.07.15.
 */
public interface UploadTransmitter {

    /**
     * UploadRequest provide inputStream supplier. Supplier get invocation will happened when upload task will be
     * executed. Transmitter will handle closing supplied input stream on success and on fail too, but only when
     * exception happens after supplier get invocation To prevent leaking, it's strongly recommended to provide lazy
     * inputStream generation. See StreamSuppliers class
     */
    ListenableFuture<UploadState> upload(@Nonnull UploadRequest request);

    /**
     * @param progressListener the listener to run when the computation is complete
     * @param notifyExecutor   the executor to run the listener in
     */
    ListenableFuture<UploadState> upload(@Nonnull UploadRequest request,
        @Nonnull Consumer<UploadState> progressListener, @Nonnull Executor notifyExecutor);
}
