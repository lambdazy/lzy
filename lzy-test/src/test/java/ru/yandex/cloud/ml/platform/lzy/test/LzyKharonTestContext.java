package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String serverAddress(boolean fromDocker);
    String servantAddress(boolean fromDocker);

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void close();
}
