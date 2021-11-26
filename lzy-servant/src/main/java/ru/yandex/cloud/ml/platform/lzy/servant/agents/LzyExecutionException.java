package ru.yandex.cloud.ml.platform.lzy.servant.agents;

public class LzyExecutionException extends Exception {
    public LzyExecutionException(Exception e) {
        super(e);
    }
}
