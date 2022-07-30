package ru.yandex.qe.s3.transfer;

/**
 * @author entropia
 */
public enum TransferAbortPolicy {
    /**
     * Transfer coordination task will exit immediately after requesting all workers to cancel.<br> This is the
     * default.
     */
    RETURN_IMMEDIATELY,
    /**
     * Transfer coordination task will only terminate after all workers have terminated.<br> Note that the transfer
     * future's {@link java.util.concurrent.Future#get() get()} method will still return instantly after the future was
     * canceled, but notification about a {@link TransferState#isFinal() final} transfer state will be sent to progress
     * listener only after all workers have finished execution.
     */
    WAIT_FOR_TERMINATION
}
