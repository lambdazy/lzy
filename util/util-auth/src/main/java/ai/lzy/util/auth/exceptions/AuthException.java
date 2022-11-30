package ai.lzy.util.auth.exceptions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public abstract class AuthException extends RuntimeException {
    private final String internalDetails;

    public AuthException(String details) {
        super("", new RuntimeException());
        this.internalDetails = details;
    }

    public AuthException(Throwable cause) {
        super(cause.getMessage(), cause);
        this.internalDetails = null;
    }

    public AuthException(Throwable cause, String details) {
        super(cause.getMessage(), cause);
        this.internalDetails = details;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public String getInternalDetails() {
        return this.internalDetails;
    }

    public abstract Status status();

    public static AuthException fromStatusRuntimeException(StatusRuntimeException e) {
        return switch (e.getStatus().getCode()) {
            case PERMISSION_DENIED -> new AuthPermissionDeniedException(e.getMessage());
            case NOT_FOUND -> new AuthNotFoundException(e.getMessage());
            case INTERNAL -> new AuthInternalException(e.getMessage());
            case UNAUTHENTICATED -> new AuthUnauthenticatedException(e.getMessage());
            case UNAVAILABLE, CANCELLED -> new AuthUnavailableException(e.getMessage());
            default -> throw new IllegalStateException("Unexpected value: " + e.getStatus());
        };
    }

    public StatusRuntimeException toStatusRuntimeException() {
        return status().withDescription(getMessage()).asRuntimeException();
    }
}
