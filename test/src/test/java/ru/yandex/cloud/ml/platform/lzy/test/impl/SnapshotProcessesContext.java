package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.test.LzySnapshotTestContext;
import ai.lzy.whiteboard.LzySnapshot;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.WbApiGrpc;

@Deprecated
public class SnapshotProcessesContext implements LzySnapshotTestContext {

    private static final long SNAPSHOT_STARTUP_TIMEOUT_SEC = 60;
    private static final int SNAPSHOT_PORT = 8999;
    private final String serverAddress;
    protected WbApiGrpc.WbApiBlockingStub lzyWhiteboardClient;
    protected SnapshotApiGrpc.SnapshotApiBlockingStub lzySnapshotClient;
    private Process lzySnapshot;
    private ManagedChannel channel;

    public SnapshotProcessesContext(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public String address() {
        init();
        return "http://localhost:" + SNAPSHOT_PORT;
    }

    @Override
    public WbApiGrpc.WbApiBlockingStub wbClient() {
        init();
        return lzyWhiteboardClient;
    }

    @Override
    public SnapshotApiGrpc.SnapshotApiBlockingStub snapshotClient() {
        init();
        return lzySnapshotClient;
    }

    @Override
    public void init() {
        if (lzyWhiteboardClient == null) {
            try {
                lzySnapshot = Utils.javaProcess(
                    LzySnapshot.class.getCanonicalName(),
                    new String[] {
                        "--port",
                        String.valueOf(SNAPSHOT_PORT),
                        "--lzy-server-address",
                        serverAddress
                    },
                    new String[] {
                        "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32",
                        "-Dsnapshot.uri=http://localhost:8999"
                    }
                ).inheritIO().start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

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
        }
    }

    @Override
    public void close() {
        if (lzySnapshot != null) {
            try {
                channel.shutdown();
                channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                lzySnapshot.destroy();
                lzySnapshot.onExit().get(Long.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
