package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

public class AuthException extends RuntimeException {
    private final String internalDetails;

    public AuthException(Throwable cause) {
        super(cause.getMessage(), cause);
        this.internalDetails = null;
    }

    public AuthException(Throwable cause, String internal) {
        super(cause.getMessage(), cause);
        this.internalDetails = internal;
    }

    public String getInternalDetails() {
        return this.internalDetails;
    }
}
