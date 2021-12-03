package ru.yandex.cloud.ml.platform.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

public interface LzySnapshotTestContext {
    String address(boolean fromDocker);

    WbApiGrpc.WbApiBlockingStub wbClient();
    SnapshotApiGrpc.SnapshotApiBlockingStub snapshotClient();

    void init();
    void close();
}
