package ai.lzy.channelmanager.test;

import ai.lzy.channelmanager.ChannelManagerApp;
import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class BaseTestWithChannelManager {

    private ApplicationContext context;
    private ChannelManagerConfig config;
    private ChannelManagerApp app;

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
            channel.awaitTermination(10, TimeUnit.SECONDS);
        }
        app.stop();
        app.awaitTermination();
        context.stop();
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var props = new YamlPropertySourceLoader().read("channel-manager",
            new FileInputStream("../channel-manager/src/main/resources/application-test.yml"));
        props.putAll(overrides);
        context = ApplicationContext.run(PropertySource.of(props));
        config = context.getBean(ChannelManagerConfig.class);
        app = context.getBean(ChannelManagerApp.class);
        app.start();
    }

    public String getAddress() {
        return config.getAddress();
    }

    public LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub getOrCreatePrivateClient(
        RenewableJwt internalUserCredentials
    ) throws IOException {
        if (channel == null) {
            channel = newGrpcChannel(config.getAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);
        }
        if (privateClient == null) {
            privateClient = newBlockingClient(LzyChannelManagerPrivateGrpc.newBlockingStub(channel),
                "AuthChannelManagerTestPrivateClient", () -> internalUserCredentials.get().token());
        }
        return privateClient;
    }

}
