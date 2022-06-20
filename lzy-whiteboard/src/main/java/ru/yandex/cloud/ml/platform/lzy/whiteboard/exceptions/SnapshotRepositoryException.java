package ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions;

import io.grpc.Status;
import io.grpc.StatusException;

public class SnapshotRepositoryException extends RuntimeException {
    private final StatusException exception;

    public SnapshotRepositoryException(String errorMessage) {
        super(errorMessage);
        exception = Status.INTERNAL.withDescription(getMessage()).asException();
    }

    public SnapshotRepositoryException(Throwable err) {
        super(err);
        if (err instanceof StatusException) {
            exception = (StatusException) err;
        } else {
            exception = Status.INTERNAL.withDescription(getMessage()).asException();
        }
    }

    public StatusException statusException() {
        return exception;
    }
}