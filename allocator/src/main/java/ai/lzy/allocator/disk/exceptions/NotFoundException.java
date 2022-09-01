package ai.lzy.allocator.disk.exceptions;

public class NotFoundException extends Exception {
    public NotFoundException() {}

    public NotFoundException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
