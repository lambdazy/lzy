package ai.lzy.allocator.alloc.exceptions;

public class InvalidConfigurationException extends Exception {

    public InvalidConfigurationException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
