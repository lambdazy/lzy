package ai.lzy.test;

import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String serverAddress();

    String servantAddress();

    String servantFsAddress();

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void init();

    void close();
}
