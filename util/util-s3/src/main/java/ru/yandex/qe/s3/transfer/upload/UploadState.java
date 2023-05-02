package ru.yandex.qe.s3.transfer.upload;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import ru.yandex.qe.s3.transfer.TransferState;
import ru.yandex.qe.s3.transfer.TransferStatistic;
import ru.yandex.qe.s3.transfer.TransferStatus;

import javax.annotation.concurrent.Immutable;

/**
 * Established by terry on 18.01.16.
 */
@Immutable
public class UploadState implements TransferState {

    private final TransferStatus transferStatus;
    private final TransferStatistic transferStatistic;
    private final UploadRequest uploadRequest;

    public UploadState(@Nonnull TransferStatus transferStatus, @Nullable TransferStatistic transferStatistic,
        @Nonnull UploadRequest uploadRequest) {
        this.transferStatus = transferStatus;
        this.transferStatistic = transferStatistic;
        this.uploadRequest = uploadRequest;
    }

    @Nonnull
    @Override
    public TransferStatus getTransferStatus() {
        return transferStatus;
    }

    @Nullable
    @Override
    public TransferStatistic getTransferStatistic() {
        return transferStatistic;
    }

    @Nonnull
    public UploadRequest getUploadRequest() {
        return uploadRequest;
    }

    @Override
    public String toString() {
        return "UploadState{"
            + "transferStatus=" + transferStatus
            + ", transferStatistic=" + transferStatistic
            + ", uploadRequest=" + uploadRequest
            + '}';
    }
}
