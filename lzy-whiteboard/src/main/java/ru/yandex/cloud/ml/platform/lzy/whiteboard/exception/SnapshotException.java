package ru.yandex.cloud.ml.platform.lzy.whiteboard.exception;

public class SnapshotException extends RuntimeException {
    public SnapshotException(String errorMessage) {
        super(errorMessage);
    }

    public SnapshotException(String errorMessage, Throwable e) {
        super(errorMessage, e);
    }

    public SnapshotException(Throwable e) {
        super(e);
    }
}
