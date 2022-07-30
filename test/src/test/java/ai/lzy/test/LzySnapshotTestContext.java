package ai.lzy.test;

import ai.lzy.v1.SnapshotApiGrpc;
import ai.lzy.v1.WbApiGrpc;

public interface LzySnapshotTestContext {
    String address();

    WbApiGrpc.WbApiBlockingStub wbClient();

    SnapshotApiGrpc.SnapshotApiBlockingStub snapshotClient();

    void init();

    void close();
}
