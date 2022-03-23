package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

public class AuthInternalException extends AuthException {

    public AuthInternalException(Throwable cause) {
        super(cause);
    }

    public AuthInternalException(Throwable cause, String internal) {
        super(cause, internal);
    }
}
