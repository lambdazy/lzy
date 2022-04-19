package ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions;

import io.grpc.Status;

public class AuthPermissionDeniedException extends AuthException {

    public AuthPermissionDeniedException(String details) {
        super(details);
    }

    public AuthPermissionDeniedException(Throwable cause) {
        super(cause);
    }

    public AuthPermissionDeniedException(Throwable cause, String details) {
        super(cause, details);
    }

    @Override
    public Status status() {
        return Status.PERMISSION_DENIED;
    }
}
