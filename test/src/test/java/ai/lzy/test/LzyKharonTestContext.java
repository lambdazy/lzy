package ai.lzy.test;

import ai.lzy.v1.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String serverAddress();

    String servantAddress();

    String servantFsAddress();

    String channelManagerProxyAddress();

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void init();

    void close();
}
