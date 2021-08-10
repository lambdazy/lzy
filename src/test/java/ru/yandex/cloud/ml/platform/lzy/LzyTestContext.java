package ru.yandex.cloud.ml.platform.lzy;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public interface LzyTestContext {
    void start();
    void stop();

    LzyServerGrpc.LzyServerBlockingStub server();

    void startTerminalAtPathAndPort(String path, int port);
    boolean waitForServants(long timeout, TimeUnit unit, int... ports);
    boolean pathExists(Path path);
}
