package ai.lzy.test;

import ai.lzy.v1.channel.LzyChannelManagerGrpc;

public interface LzyChannelManagerContext extends AutoCloseable {
    String address();

    LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client();

    void init(boolean stubIam);

    void close();
}
