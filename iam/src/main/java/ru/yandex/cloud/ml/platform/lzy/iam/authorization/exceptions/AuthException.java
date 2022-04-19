package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

import io.grpc.Status;

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
}
