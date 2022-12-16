package ai.lzy.test.impl.v2;

import ai.lzy.channelmanager.ChannelManagerApp;
import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.test.impl.Utils;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class ChannelManagerContext {
    private static final int PORT = 18122;

    private final HostAndPort address;
    private final ApplicationContext context;
    private final ChannelManagerApp channelManager;

    private final ManagedChannel channel;
    private final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient;

    @Inject
    public ChannelManagerContext(IamContext iam) {
        this.address = HostAndPort.fromParts("localhost", PORT);

        final var opts = Utils.createModuleDatabase("channel-manager");
        opts.putAll(new HashMap<String, Object>(Map.of(
            "channel-manager.address", address.toString(),
            "channel-manager.lzy-service-address", WorkflowContext.address,
            "channel-manager.iam.address", iam.address(),
            "channel-manager.lock-buckets-count", 256,
            "channel-manager.executor-threads-count", 10,
            "channel-manager.connections.cache-concurrency-level", 10,
            "channel-manager.connections.cache-ttl", "20s"
        )));

        this.context = ApplicationContext.run(opts);
        final var config = context.getBean(ChannelManagerConfig.class);

        this.channelManager = context.getBean(ChannelManagerApp.class);
        try {
            channelManager.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final var internalUserCredentials = config.getIam().createRenewableToken();
        channel = newGrpcChannel(address, LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
            "PrivateTest", () -> internalUserCredentials.get().token());
    }

    @PreDestroy
    public void close() throws InterruptedException {
        channel.shutdown();
        channel.awaitTermination(10, TimeUnit.SECONDS);
        channelManager.stop();
        channelManager.awaitTermination();
        context.stop();
    }

    public String address() {
        return address.toString();
    }

    public LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient() {
        return privateClient;
    }
}
