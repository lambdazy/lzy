package ai.lzy.test.impl;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.channelmanager.ChannelManagerAppOld;
import ai.lzy.test.LzyChannelManagerContext;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManagerThreadContext implements LzyChannelManagerContext {
    private static final Logger LOG = LogManager.getLogger(ChannelManagerThreadContext.class);

    private final String whiteboardAddress;
    private final HostAndPort iamAddress;
    private ChannelManagerAppOld channelManager;
    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client;
    private LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient;
    private ManagedChannel channel;
    private ApplicationContext context;

    public static class Config extends Utils.Defaults {
        private static final long STARTUP_TIMEOUT_SEC = 60;
        private static final int PORT = 8122;
    }

    public ChannelManagerThreadContext(String whiteboardAddress, HostAndPort iamAddress) {
        this.whiteboardAddress = whiteboardAddress;
        this.iamAddress = iamAddress;
    }

    @Override
    public String address() {
        return "localhost:" + Config.PORT;
    }

    @Override
    public LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client() {
        return client;
    }

    @Override
    public LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub privateClient() {
        return privateClient;
    }

    public void init(boolean stubIam) {
        LOG.info("Starting channel-manager...");

        var props = Utils.loadModuleTestProperties("channel-manager");
        props.putAll(Utils.createModuleDatabase("channel-manager"));
        props.put("channel-manager.address", "localhost:" + Config.PORT);
        props.put("channel-manager.whiteboard-address", whiteboardAddress);
        props.put("channel-manager.iam.address", iamAddress);
        props.put("channel-manager.stub-iam", stubIam);

        try {
            context = ApplicationContext.run(PropertySource.of(props));

            channelManager = new ChannelManagerAppOld(context);
            channelManager.start();

            channel = ChannelBuilder
                .forAddress("localhost", Config.PORT)
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                .build();
            client = LzyChannelManagerGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(Config.STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);
            var creds = context.getBean(ServiceConfig.class).getIam().createRenewableToken();
            privateClient = LzyChannelManagerPrivateGrpc.newBlockingStub(channel)
                .withInterceptors(ClientHeaderInterceptor.authorization(() -> creds.get().token()));
            while (channel.getState(true) != ConnectivityState.READY) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        } catch (IOException e) {
            LOG.fatal("Failed to start channel-manager: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            channelManager.stop();
            channelManager.awaitTermination();
            context.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
