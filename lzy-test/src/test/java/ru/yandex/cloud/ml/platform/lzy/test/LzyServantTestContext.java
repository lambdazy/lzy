package ru.yandex.cloud.ml.platform.lzy.test;

import java.nio.file.Path;

public interface LzyServantTestContext extends AutoCloseable {
    Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort);

    boolean inDocker();
    void close();

    interface Servant {
        boolean pathExists(Path path);
        String execute(String... command);
    }
}
