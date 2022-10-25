package ai.lzy.test.impl;

import ai.lzy.test.LzySnapshotTestContext;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.deprecated.SnapshotApiGrpc;
import ai.lzy.v1.deprecated.WbApiGrpc;
import ai.lzy.whiteboard.api.SnapshotApi;
import ai.lzy.whiteboard.api.WhiteboardApi;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SnapshotThreadContext implements LzySnapshotTestContext {

    private static final long SNAPSHOT_STARTUP_TIMEOUT_SEC = 60;
    private static final int SNAPSHOT_PORT = 8999;
    private final String serverAddress;
    protected WbApiGrpc.WbApiBlockingStub lzyWhiteboardClient;
    protected SnapshotApiGrpc.SnapshotApiBlockingStub lzySnapshotClient;
    private Server server;
    private ManagedChannel channel;

    public SnapshotThreadContext(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public String address() {
        return "http://localhost:" + SNAPSHOT_PORT;
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
                                "service.server-uri", serverAddress
                        )
                )
        ))
        {
            var logger = LogManager.getLogger(SnapshotApi.class);
            logger.info("Starting LzySnapshot and LzyWhiteboard on port {}...", SNAPSHOT_PORT);

            SnapshotApi spImpl = context.getBean(SnapshotApi.class);
            WhiteboardApi wbImpl = context.getBean(WhiteboardApi.class);
            ServerBuilder<?> builder = ServerBuilder.forPort(SNAPSHOT_PORT)
                .addService(wbImpl)
                .addService(spImpl);

            server = builder.build();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("gRPC server is shutting down!");
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
