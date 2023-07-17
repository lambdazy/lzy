package ai.lzy.allocator.exceptions;

public class NetworkPolicyException extends RuntimeException {

    private final boolean retryable;

    public NetworkPolicyException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
