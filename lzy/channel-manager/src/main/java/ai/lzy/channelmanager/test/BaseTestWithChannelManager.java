package ai.lzy.channelmanager.test;

import ai.lzy.channelmanager.ChannelManagerApp;
import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.annotation.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class BaseTestWithChannelManager {

    private ApplicationContext context;
    private ChannelManagerConfig config;

    @Nullable
    private ManagedChannel channel;
    @Nullable
    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient;

    public void before() throws IOException {
        setUp(Map.of());
    }

    public void after() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                // intentionally blank
            } finally {
                channel.shutdownNow();
            }
        }

        context.stop();
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var props = new YamlPropertySourceLoader().read("channel-manager",
            new FileInputStream("../channel-manager/src/main/resources/application-test.yml"));
        props.putAll(overrides);

        context = ApplicationContext.run(PropertySource.of(props), "test-mock");
        context.getBean(ChannelManagerDataSource.class).setOnClose(DatabaseTestUtils::cleanup);

        config = context.getBean(ChannelManagerConfig.class);

        ChannelManagerApp app = context.getBean(ChannelManagerApp.class);
        app.start();

        channel = null;
        privateClient = null;
    }

    public String getAddress() {
        return config.getAddress();
    }

    public LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub getOrCreatePrivateClient(
        RenewableJwt internalUserCredentials)
    {
        if (channel == null || channel.isShutdown()) {
            channel = newGrpcChannel(config.getAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);
            privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
                "AuthChannelManagerTestPrivateClient", () -> internalUserCredentials.get().token());
        }
        return privateClient;
    }

    public ApplicationContext getContext() {
        return context;
    }
}
