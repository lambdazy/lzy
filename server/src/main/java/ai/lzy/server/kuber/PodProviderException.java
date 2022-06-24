package ai.lzy.server.kuber;

public class PodProviderException extends Exception {
    public PodProviderException() {
    }

    public PodProviderException(String message) {
        super(message);
    }

    public PodProviderException(String message, Throwable cause) {
        super(message, cause);
    }

    public PodProviderException(Throwable cause) {
        super(cause);
    }

    public PodProviderException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
