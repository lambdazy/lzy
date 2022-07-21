package ai.lzy.test;

import ai.lzy.v1.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String serverAddress();

    String servantAddress();

    String servantFsAddress();

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void init();

    void close();
}
