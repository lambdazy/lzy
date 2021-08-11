package ru.yandex.cloud.ml.platform.lzy;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.nio.file.Path;

public interface LzyTestContext {
    void start();
    void stop();

    LzyServerGrpc.LzyServerBlockingStub server();

    boolean startTerminalAtPathAndPort(String path, int port);
    boolean pathExists(Path path);
}
