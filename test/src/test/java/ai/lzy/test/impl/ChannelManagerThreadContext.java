package ai.lzy.test.impl;

import ai.lzy.channelmanager.ChannelManager;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.LzyKharonGrpc;
import ai.lzy.test.ChannelManagerContext;
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManagerThreadContext implements ChannelManagerContext {
    private final String whiteboardAddress;
    private final HostAndPort iamAddress;
    private ChannelManager channelManager;
    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client;
    private ManagedChannel channel;

    static class Config extends Utils.Defaults {
        private static final long STARTUP_TIMEOUT_SEC = 60;
        private static final int PORT = 8122;
    }

    public ChannelManagerThreadContext(String whiteboardAddress, HostAndPort iamAddress) {
        this.whiteboardAddress = whiteboardAddress;
        this.iamAddress = iamAddress;
    }

    @Override
    public String address() {
        return "http://localhost:" + Config.PORT;
    }

    @Override
    public LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client() {
        return client;
    }

    @Override
    public void init() {
        Map<String, Object> appProperties = Map.of(
            "channel-manager.address", "localhost:" + Config.PORT,
            "channel-manager.whiteboard-address", whiteboardAddress,
            "channel-manager.iam.address", iamAddress
        );
        try (ApplicationContext context = ApplicationContext.run(PropertySource.of(appProperties))) {
            channelManager = new ChannelManager(context);
            channelManager.start();

            channel = ChannelBuilder
                .forAddress("localhost", Config.PORT)
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                .build();
            client = LzyChannelManagerGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(Config.STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);
            while (channel.getState(true) != ConnectivityState.READY) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            channelManager.close();
            channelManager.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
