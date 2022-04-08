package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.api.SnapshotApi;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.api.WhiteboardApi;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LzySnapshotThreadContext implements LzySnapshotTestContext {

    private static final long SNAPSHOT_STARTUP_TIMEOUT_SEC = 60;
    private static final int SNAPSHOT_PORT = 8999;
    private final String serverAddress;
    protected WbApiGrpc.WbApiBlockingStub lzyWhiteboardClient;
    protected SnapshotApiGrpc.SnapshotApiBlockingStub lzySnapshotClient;
    private Server server;
    private ManagedChannel channel;

    public LzySnapshotThreadContext(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public String address(boolean fromDocker) {
        if (!SystemUtils.IS_OS_LINUX && fromDocker) {
            return "http://host.docker.internal:" + SNAPSHOT_PORT;
        } else {
            return "http://localhost:" + SNAPSHOT_PORT;
        }
    }

    @Override
    public WbApiGrpc.WbApiBlockingStub wbClient() {
        return lzyWhiteboardClient;
    }

    @Override
    public SnapshotApiGrpc.SnapshotApiBlockingStub snapshotClient() {
        return lzySnapshotClient;
    }

    @Override
    public void init() {
        try (ApplicationContext context = ApplicationContext.run(
                PropertySource.of(
                        Map.of(
                                "snapshot.uri", address(false),
                                "server.uri", serverAddress
                        )
                )
        )) {
            SnapshotApi spImpl = context.getBean(SnapshotApi.class);
            WhiteboardApi wbImpl = context.getBean(WhiteboardApi.class);
            ServerBuilder<?> builder = ServerBuilder.forPort(SNAPSHOT_PORT)
                    .addService(wbImpl);
            builder.addService(spImpl);

            server = builder.build();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("gRPC server is shutting down!");
                server.shutdown();
            }));

            channel = ChannelBuilder
                    .forAddress("localhost", SNAPSHOT_PORT)
                    .usePlaintext()
                    .enableRetry(WbApiGrpc.SERVICE_NAME)
                    .build();
            lzyWhiteboardClient = WbApiGrpc.newBlockingStub(channel)
                    .withWaitForReady()
                    .withDeadlineAfter(SNAPSHOT_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);

            lzySnapshotClient = SnapshotApiGrpc.newBlockingStub(channel)
                    .withWaitForReady()
                    .withDeadlineAfter(SNAPSHOT_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);

            while (channel.getState(true) != ConnectivityState.READY) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
