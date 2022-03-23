package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

public class AuthPermissionDeniedException extends AuthException {

    public AuthPermissionDeniedException(Throwable cause) {
        super(cause);
    }

    public AuthPermissionDeniedException(Throwable cause, String internal) {
        super(cause, internal);
    }
}
