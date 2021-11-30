package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.WhiteboardApiGrpc;

public interface LzyWhiteboardTestContext {
    String address(boolean fromDocker);

    WhiteboardApiGrpc.WhiteboardApiBlockingStub client();

    void init();
    void close();
}
