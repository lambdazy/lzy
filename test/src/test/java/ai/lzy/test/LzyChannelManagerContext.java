package ai.lzy.test;

import ai.lzy.v1.channel.deprecated.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.deprecated.LzyChannelManagerPrivateGrpc;

public interface LzyChannelManagerContext extends AutoCloseable {
    String address();

    LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client();
    LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient();

    void init(boolean stubIam);

    void close();
}
