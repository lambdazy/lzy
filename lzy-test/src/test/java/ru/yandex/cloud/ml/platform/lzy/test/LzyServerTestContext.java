package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

public interface LzyServerTestContext extends AutoCloseable {
    String address();

    LzyServerGrpc.LzyServerBlockingStub client();

    void init();

    void close();
}
