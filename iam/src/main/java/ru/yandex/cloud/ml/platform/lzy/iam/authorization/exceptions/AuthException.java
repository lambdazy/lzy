package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

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

    public String getInternalDetails() {
        return this.internalDetails;
    }

    public abstract Status status();

    public static AuthException fromStatusRuntimeException(StatusRuntimeException e) {
        AuthException exception;
        if (Status.PERMISSION_DENIED.equals(e.getStatus())) {
            exception = new AuthPermissionDeniedException(e.getMessage());
        } else if (Status.INVALID_ARGUMENT.equals(e.getStatus())) {
            exception = new AuthBadRequestException(e.getMessage());
        } else if (Status.INTERNAL.equals(e.getStatus())) {
            exception = new AuthInternalException(e.getMessage());
        } else if (Status.UNAUTHENTICATED.equals(e.getStatus())) {
            exception = new AuthUnauthenticatedException(e.getMessage());
        } else if (Status.UNAVAILABLE.equals(e.getStatus())) {
            exception = new AuthUnavailableException(e.getMessage());
        } else {
            throw new IllegalStateException("Unexpected value: " + e.getStatus());
        }
        return exception;
    }
}
