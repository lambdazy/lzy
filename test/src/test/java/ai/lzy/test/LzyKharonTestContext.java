package ai.lzy.test;

import ai.lzy.v1.deprecated.LzyKharonGrpc;

public interface LzyKharonTestContext {
    String serverAddress();

    String servantAddress();

    String servantFsAddress();

    String channelManagerProxyAddress();

    LzyKharonGrpc.LzyKharonBlockingStub client();

    void init();

    void close();
}
