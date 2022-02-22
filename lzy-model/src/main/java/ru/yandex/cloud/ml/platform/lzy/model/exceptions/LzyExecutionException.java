package ru.yandex.cloud.ml.platform.lzy.model.exceptions;

public class LzyExecutionException extends Exception {
    public LzyExecutionException(Exception e) {
        super(e);
    }
}
