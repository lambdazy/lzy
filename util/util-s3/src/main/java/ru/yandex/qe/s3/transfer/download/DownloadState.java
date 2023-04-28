package ru.yandex.qe.s3.transfer.download;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import ru.yandex.qe.s3.transfer.TransferState;
import ru.yandex.qe.s3.transfer.TransferStatistic;
import ru.yandex.qe.s3.transfer.TransferStatus;
import ru.yandex.qe.s3.transfer.meta.Metadata;

import javax.annotation.concurrent.Immutable;

/**
 * Established by terry on 18.01.16.
 */
@Immutable
public class DownloadState implements TransferState {

    private final TransferStatus transferStatus;
    private final TransferStatistic transferStatistic;

    private final DownloadRequest downloadRequest;

    private final Metadata objectMetadata;

    public DownloadState(@Nonnull TransferStatus transferStatus, @Nullable TransferStatistic transferStatistic,
        @Nonnull DownloadRequest downloadRequest, @Nullable Metadata objectMetadata) {
        this.transferStatus = transferStatus;
        this.transferStatistic = transferStatistic;
        this.downloadRequest = downloadRequest;
        this.objectMetadata = objectMetadata;
    }

    @Nonnull
    @Override
    public TransferStatus getTransferStatus() {
        return transferStatus;
    }

    @Override
    public TransferStatistic getTransferStatistic() {
        return transferStatistic;
    }

    @Nonnull
    public DownloadRequest getDownloadRequest() {
        return downloadRequest;
    }

    @Nullable
    public Metadata getObjectMetadata() {
        return objectMetadata;
    }

    @Override
    public String toString() {
        return "DownloadState{"
            + "transferStatus=" + transferStatus
            + ", transferStatistic=" + transferStatistic
            + ", downloadRequest=" + downloadRequest
            + ", objectMetadata=" + objectMetadata
            + '}';
    }
}
