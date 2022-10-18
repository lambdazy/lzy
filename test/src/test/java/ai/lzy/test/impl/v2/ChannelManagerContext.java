package ai.lzy.test.impl.v2;

import ai.lzy.test.impl.ChannelManagerThreadContext;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

@Singleton
public class ChannelManagerContext {

    private final ChannelManagerThreadContext ctx;

    public ChannelManagerContext(IamContext iam) {
        ctx = new ChannelManagerThreadContext("localhost:12345", iam.address());
        ctx.init(false);
    }

    @PreDestroy
    public void close() {
        ctx.close();
    }

    public String address() {
        return ctx.address();
    }

    public LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient() {
        return ctx.privateClient();
    }
}
