package ai.lzy.allocator.disk.util;

public class OperationTimedOutException extends RuntimeException {
    public OperationTimedOutException(String message, Throwable cause) {
        super(message, cause);
    }
}
