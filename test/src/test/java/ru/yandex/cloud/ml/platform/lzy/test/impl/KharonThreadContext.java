package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;
import ru.yandex.cloud.ml.platform.lzy.kharon.LzyKharon;
import ru.yandex.cloud.ml.platform.lzy.model.UriScheme;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import ai.lzy.whiteboard.api.SnapshotApi;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class KharonThreadContext implements LzyKharonTestContext {

    private static final long KHARON_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_KHARON_PORT = 8899;
    private static final int LZY_KHARON_SERVANT_PROXY_PORT = 8900;
    private static final int LZY_KHARON_SERVANT_FS_PROXY_PORT = 8901;

    private final String serverAddress;
    private final String whiteboardAddress;
    private LzyKharon kharon;
    private ManagedChannel channel;
    private LzyKharonGrpc.LzyKharonBlockingStub lzyKharonClient;

    public KharonThreadContext(String serverAddress, String whiteboardAddress) {
        var sa = URI.create(serverAddress);
        var wa = URI.create(whiteboardAddress);
        this.serverAddress = sa.getHost() + ":" + sa.getPort();
        this.whiteboardAddress = wa.getHost() + ":" + wa.getPort();
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
    public String servantFsAddress() {
        return UriScheme.LzyFs.scheme() + "localhost:" + LZY_KHARON_SERVANT_FS_PROXY_PORT;
    }

    @Override
    public LzyKharonGrpc.LzyKharonBlockingStub client() {
        return lzyKharonClient;
    }

    @Override
    public void init() {
        Map<String, Object> appProperties = Map.of(
                "kharon.address", "localhost:" + LZY_KHARON_PORT,
                "kharon.external-host", "localhost",
                "kharon.server-address", serverAddress,
                "kharon.whiteboard-address", whiteboardAddress,
                "kharon.snapshot-address", whiteboardAddress,
                "kharon.servant-proxy-port", LZY_KHARON_SERVANT_PROXY_PORT,
                "kharon.servant-proxy-fs-port", LZY_KHARON_SERVANT_FS_PROXY_PORT
                //"kharon.workflow", null
        );
        try (ApplicationContext context = ApplicationContext.run(PropertySource.of(appProperties))) {
            var logger = LogManager.getLogger(SnapshotApi.class);
            logger.info("Starting LzyKharon on port {}...", LZY_KHARON_PORT);

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
        }
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            kharon.close();
            kharon.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
