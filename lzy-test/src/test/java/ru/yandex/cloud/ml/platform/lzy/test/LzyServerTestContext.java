package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.util.concurrent.TimeUnit;

public interface LzyServerTestContext extends AutoCloseable {
    String host(boolean fromDocker);
    int port();

    LzyServerGrpc.LzyServerBlockingStub client();
    boolean waitForServants(long timeout, TimeUnit unit, int... ports);

    void close();
}
