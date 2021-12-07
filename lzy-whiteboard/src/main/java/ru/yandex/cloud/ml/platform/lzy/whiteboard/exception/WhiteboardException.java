package ru.yandex.cloud.ml.platform.lzy.whiteboard.exception;

public class WhiteboardException extends RuntimeException {
    public WhiteboardException(String errorMessage) {
        super(errorMessage);
    }

    public WhiteboardException(String errorMessage, Throwable e) {
        super(errorMessage, e);
    }

    public WhiteboardException(Throwable e) {
        super(e);
    }
}
