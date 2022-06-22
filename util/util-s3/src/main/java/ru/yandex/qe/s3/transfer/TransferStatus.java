package ru.yandex.qe.s3.transfer;

/**
 * Established by terry on 20.01.16.
 */
public enum TransferStatus {
    /**
     * Transfer has just started. This is an {@link #isInitial() initial} status.
     */
    STARTED,
    /**
     * Some data has been transferred.
     * <br>Note that the transfer can just go from an {@link #isInitial() initial} status to
     * a {@link #isFinal() final} one without passing through {@code IN_PROGRESS}, e.g., for very short transfers.
     */
    IN_PROGRESS,
    /**
     * Transfer finished successfully.
     * <br>This is a {@link #isFinal() final} status; no further transfer state change
     * notifications will be delivered after a notification with this status.
     */
    DONE,
    /**
     * Transfer failed.
     * <br>This is a {@link #isFinal() final} status; no further transfer state change
     * notifications will be delivered after a notification with this status.
     */
    FAILED,
    /**
     * Transfer was canceled.
     * <br>This is a {@link #isFinal() final} status; no further transfer state change
     * notifications will be delivered after a notification with this status.
     */
    CANCELED;

    /**
     * @return {@code true} if the transfer's status won't change further; {@code false} otherwise
     */
    public boolean isFinal() {
        return this == DONE || this == FAILED || this == CANCELED;
    }

    /**
     * @return {@code true} if this is an <em>initial</em> state, i.e., no data has yet been transferred;<br> {@code
     * false} otherwise
     */
    public boolean isInitial() {
        return this == STARTED;
    }
}
