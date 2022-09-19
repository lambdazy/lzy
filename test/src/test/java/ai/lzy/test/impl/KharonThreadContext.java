package ai.lzy.test.impl;

import ai.lzy.kharon.LzyKharon;
import ai.lzy.model.UriScheme;
import ai.lzy.test.LzyKharonTestContext;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class KharonThreadContext implements LzyKharonTestContext {
    private static final Logger LOG = LogManager.getLogger(KharonThreadContext.class);

    private static final long KHARON_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_KHARON_PORT = 8899;
    private static final int LZY_KHARON_SERVANT_PROXY_PORT = 8900;
    private static final int LZY_KHARON_SERVANT_FS_PROXY_PORT = 8901;
    private static final int LZY_KHARON_CHANNEL_MANAGER_PROXY_PORT = 8123;

    private final String serverAddress;
    private final String whiteboardAddress;
    private final String channelManagerAddress;
    private final String channelManagerProxyAddress;
    private final String iamAddress;
    private ApplicationContext context;
    private LzyKharon kharon;
    private ManagedChannel channel;
    private LzyKharonGrpc.LzyKharonBlockingStub lzyKharonClient;

    public KharonThreadContext(String serverAddress, String whiteboardAddress, String channelManagerAddress,
                               HostAndPort iamAddress)
    {
        var sa = URI.create(serverAddress);
        var wa = URI.create(whiteboardAddress);
        var parsedChannelManagerAddress = URI.create(channelManagerAddress);
        this.serverAddress = sa.getHost() + ":" + sa.getPort();
        this.whiteboardAddress = wa.getHost() + ":" + wa.getPort();
        this.channelManagerAddress = parsedChannelManagerAddress.getHost()
            + ":" + parsedChannelManagerAddress.getPort();
        channelManagerProxyAddress = "ch-man://" + parsedChannelManagerAddress.getHost()
            + ":" + LZY_KHARON_CHANNEL_MANAGER_PROXY_PORT;
        this.iamAddress = iamAddress.toString();
    }

    @Override
    public String serverAddress() {
        return "http://localhost:" + LZY_KHARON_PORT;
    }

    @Override
    public String servantAddress() {
        return UriScheme.LzyServant.scheme() + "localhost:" + LZY_KHARON_SERVANT_PROXY_PORT;
    }

    @Override
    public String channelManagerProxyAddress() {
        return channelManagerProxyAddress;
    }

    @Override
    public String servantFsAddress() {
        return UriScheme.LzyFs.scheme() + "localhost:" + LZY_KHARON_SERVANT_FS_PROXY_PORT;
    }

    @Override
    public LzyKharonGrpc.LzyKharonBlockingStub client() {
        return lzyKharonClient;
    }

    @Override
    public void init() {
        var props = Utils.loadModuleTestProperties("kharon");
        props.putAll(Utils.createModuleDatabase("kharon"));

        props.put("kharon.address", "localhost:" + LZY_KHARON_PORT);
        props.put("kharon.external-host", "localhost");
        props.put("kharon.server-address", serverAddress);
        props.put("kharon.whiteboard-address", whiteboardAddress);
        props.put("kharon.snapshot-address", whiteboardAddress);
        props.put("kharon.channel-manager-address", channelManagerAddress);
        props.put("kharon.servant-proxy-port", LZY_KHARON_SERVANT_PROXY_PORT);
        props.put("kharon.servant-fs-proxy-port", LZY_KHARON_SERVANT_FS_PROXY_PORT);
        props.put("kharon.channel-manager-proxy-port", LZY_KHARON_CHANNEL_MANAGER_PROXY_PORT);

        props.put("kharon.iam.address", iamAddress);
        props.put("kharon.storage.address", "localhost:" + StorageThreadContext.STORAGE_PORT);
        props.put("kharon.workflow.enabled", "false");

        LOG.info("Starting LzyKharon on port {}...", LZY_KHARON_PORT);

        try {
            context = ApplicationContext.run(PropertySource.of(props));

            try {
                kharon = new LzyKharon(context);
                kharon.start();
            } catch (URISyntaxException | IOException e) {
                throw new RuntimeException(e);
            }
            channel = ChannelBuilder
                    .forAddress("localhost", LZY_KHARON_PORT)
                    .usePlaintext()
                    .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                    .build();
            lzyKharonClient = LzyKharonGrpc.newBlockingStub(channel)
                    .withWaitForReady()
                    .withDeadlineAfter(KHARON_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);
            while (channel.getState(true) != ConnectivityState.READY) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        } catch (Exception e) {
            LOG.fatal("Failed to start Kharon: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            kharon.close();
            kharon.awaitTermination();
            context.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
