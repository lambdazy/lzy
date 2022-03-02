package ru.yandex.cloud.ml.platform.lzy.model.exceptions;

import java.io.IOException;

public class EnvironmentInstallationException extends Exception {
    public EnvironmentInstallationException(String message) {
        super(message);
    }

    public EnvironmentInstallationException(Exception e) {
        super(e);
    }
}
