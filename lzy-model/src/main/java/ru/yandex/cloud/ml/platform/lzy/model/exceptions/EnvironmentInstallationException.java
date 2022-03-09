package ru.yandex.cloud.ml.platform.lzy.model.exceptions;

public class EnvironmentInstallationException extends Exception {
    public EnvironmentInstallationException(String message) {
        super(message);
    }

    public EnvironmentInstallationException(Exception e) {
        super(e);
    }
}
