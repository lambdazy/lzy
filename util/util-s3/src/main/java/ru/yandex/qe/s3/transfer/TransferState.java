package ru.yandex.qe.s3.transfer;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * @author entropia
 */
public interface TransferState {

    @Nonnull
    TransferStatus getTransferStatus();

    @Nullable
    TransferStatistic getTransferStatistic();

    default boolean isFinal() {
        return getTransferStatus().isFinal();
    }

    default boolean isInitial() {
        return getTransferStatus().isInitial();
    }
}
