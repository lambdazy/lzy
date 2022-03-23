package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

public class AuthBadRequestException extends AuthException {

    public AuthBadRequestException(Throwable cause) {
        super(cause);
    }

    public AuthBadRequestException(Throwable cause, String internal) {
        super(cause, internal);
    }
}
