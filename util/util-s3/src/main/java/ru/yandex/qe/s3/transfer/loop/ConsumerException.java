package ru.yandex.qe.s3.transfer.loop;

/**
 * Thrown if consumer don't read whole stream or has thrown exception while processing data. In latter case original
 * exception could be obtained via {@link #getCause()}.
 * <p>
 * Established by terry on 22.07.15.
 */
public class ConsumerException extends RuntimeException {

    public ConsumerException(String message) {
        super(message);
    }

    public ConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
