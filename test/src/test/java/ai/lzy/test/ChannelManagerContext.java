package ai.lzy.test;

import ai.lzy.v1.LzyChannelManagerGrpc;

public interface ChannelManagerContext extends AutoCloseable {
    String address();

    LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client();

    void init();

    void close();
}
