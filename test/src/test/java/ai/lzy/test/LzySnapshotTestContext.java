package ai.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

public interface LzySnapshotTestContext {
    String address();

    WbApiGrpc.WbApiBlockingStub wbClient();

    SnapshotApiGrpc.SnapshotApiBlockingStub snapshotClient();

    void init();

    void close();
}
