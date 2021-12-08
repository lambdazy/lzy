package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import java.io.IOException;

public class EnvironmentInstallationException extends Exception {
    public EnvironmentInstallationException(String message) {
        super(message);
    }

    public EnvironmentInstallationException(IOException e) {
        super(e);
    }
}
