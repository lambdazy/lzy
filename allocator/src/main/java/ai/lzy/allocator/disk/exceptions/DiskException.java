package ai.lzy.allocator.disk.exceptions;

public class DiskException extends RuntimeException {
    public DiskException(String message) {
        super(message);
    }

    public DiskException(Throwable cause) {
        super(cause);
    }

    public DiskException(String message, Throwable cause) {
        super(message, cause);
    }
}
