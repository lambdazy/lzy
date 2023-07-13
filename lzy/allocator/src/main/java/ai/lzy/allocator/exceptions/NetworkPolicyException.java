package ai.lzy.allocator.exceptions;

public class NetworkPolicyException extends RuntimeException {

    private final boolean retryable;

    public NetworkPolicyException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public synchronized boolean isRetryable() {
        return retryable;
    }
}
