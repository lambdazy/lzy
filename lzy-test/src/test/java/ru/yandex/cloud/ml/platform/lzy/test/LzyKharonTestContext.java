package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String host(boolean fromDocker);
    int port();

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void close();
}
