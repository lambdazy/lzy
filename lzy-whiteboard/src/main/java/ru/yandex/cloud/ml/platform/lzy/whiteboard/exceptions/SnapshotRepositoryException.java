package ru.yandex.cloud.ml.platform.lzy.whiteboard.exceptions;

public class SnapshotRepositoryException extends Exception {
    public SnapshotRepositoryException(String errorMessage) {
        super(errorMessage);
    }

    public SnapshotRepositoryException(Throwable err) {
        super(err);
    }
}