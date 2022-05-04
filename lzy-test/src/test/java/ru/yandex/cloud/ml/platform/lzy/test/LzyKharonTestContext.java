package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String serverAddress();

    String servantAddress();

    String servantFsAddress();

    String servantFsAddress(boolean fromDocker);

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void init();

    void close();
}
