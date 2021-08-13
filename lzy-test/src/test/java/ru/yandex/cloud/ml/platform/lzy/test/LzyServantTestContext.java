package ru.yandex.cloud.ml.platform.lzy.test;

import ru.yandex.cloud.ml.platform.lzy.servant.ServantStatus;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public interface LzyServantTestContext extends AutoCloseable {
    Servant startTerminalAtPathAndPort(String path, int port, String serverHost, int serverPort);

    boolean inDocker();
    void close();

    interface Servant {
        boolean pathExists(Path path);
        ExecutionResult execute(String... command);

        boolean waitForStatus(ServantStatus status, long timeout, TimeUnit unit);
        boolean waitForShutdown(long timeout, TimeUnit unit);

        interface ExecutionResult {
            String stdout();
            String stderr();
            int exitCode();
        }
    }
}
