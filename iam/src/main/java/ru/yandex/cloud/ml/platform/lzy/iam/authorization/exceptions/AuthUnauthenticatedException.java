package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

public class AuthUnauthenticatedException extends AuthException {

    public AuthUnauthenticatedException(Throwable cause) {
        super(cause);
    }

    public AuthUnauthenticatedException(Throwable cause, String internal) {
        super(cause, internal);
    }
}
