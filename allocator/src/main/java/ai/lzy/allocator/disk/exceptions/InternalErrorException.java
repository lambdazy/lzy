package ai.lzy.allocator.disk.exceptions;

public class InternalErrorException extends Exception {
    public InternalErrorException(String message) {
        super(message);
    }

    public InternalErrorException(Throwable cause) {
        super(cause);
    }

    public InternalErrorException(String message, Throwable cause) {
        super(message, cause);
    }
}
