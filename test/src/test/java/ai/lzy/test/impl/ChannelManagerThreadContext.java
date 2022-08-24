package ai.lzy.test.impl;

import ai.lzy.channelmanager.ChannelManager;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.LzyKharonGrpc;
import ai.lzy.test.ChannelManagerContext;
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManagerThreadContext implements ChannelManagerContext {
    private static final Logger LOG = LogManager.getLogger(ChannelManagerThreadContext.class);

    private final String whiteboardAddress;
    private final HostAndPort iamAddress;
    private ChannelManager channelManager;
    private LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client;
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
        return "http://localhost:" + Config.PORT;
    }

    @Override
    public LzyChannelManagerGrpc.LzyChannelManagerBlockingStub client() {
        return client;
    }

    @Override
    public void init() {
        LOG.info("Starting channel-manager...");

        var props = Utils.loadModuleTestProperties("channel-manager");
        props.put("channel-manager.address", "localhost:" + Config.PORT);
        props.put("channel-manager.whiteboard-address", whiteboardAddress);
        props.put("channel-manager.iam.address", iamAddress);

        try {
            context = ApplicationContext.run(PropertySource.of(props));

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
