package ai.lzy.test;

import ai.lzy.priv.v2.SnapshotApiGrpc;
import ai.lzy.priv.v2.WbApiGrpc;

public interface LzySnapshotTestContext {
    String address();

    WbApiGrpc.WbApiBlockingStub wbClient();

    SnapshotApiGrpc.SnapshotApiBlockingStub snapshotClient();

    void init();

    void close();
}
