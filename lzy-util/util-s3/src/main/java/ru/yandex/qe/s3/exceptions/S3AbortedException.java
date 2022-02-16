package ru.yandex.qe.s3.exceptions;

/**
 * Established by terry on 19.01.16.
 */
public class S3AbortedException extends S3TransferException {

    public S3AbortedException(String message) {
        super(message);
    }

    public S3AbortedException(String message, Throwable cause) {
        super(message, cause);
    }
}
