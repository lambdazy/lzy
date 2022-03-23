package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

public class AuthUnavailableException extends AuthException {

    public AuthUnavailableException(Throwable cause) {
        super(cause);
    }

    public AuthUnavailableException(Throwable cause, String internal) {
        super(cause, internal);
    }
}
