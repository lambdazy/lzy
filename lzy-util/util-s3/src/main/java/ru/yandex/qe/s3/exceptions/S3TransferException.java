package ru.yandex.qe.s3.exceptions;

/**
 * Established by terry
 * on 19.01.16.
 */
public class S3TransferException extends RuntimeException {

    public S3TransferException(String message) {
        super(message);
    }

    public S3TransferException(String message, Throwable cause) {
        super(message, cause);
    }
}
