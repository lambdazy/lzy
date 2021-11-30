package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.test.LzyWhiteboardTestContext;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.WhiteboardApi;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.WhiteboardApiGrpc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class LzyWhiteboardProcessesContext implements LzyWhiteboardTestContext {
    private static final long WHITEBOARD_STARTUP_TIMEOUT_SEC = 60;
    private static final int WHITEBOARD_PORT = 8999;
    private Process lzyWhiteboard;
    private ManagedChannel channel;
    protected WhiteboardApiGrpc.WhiteboardApiBlockingStub lzyWhiteboardClient;

    @Override
    public String address(boolean fromDocker) {
        init();
        if (!SystemUtils.IS_OS_LINUX && fromDocker) {
            return "http://host.docker.internal:" + WHITEBOARD_PORT;
        } else {
            return "http://localhost:" + WHITEBOARD_PORT;
        }
    }

    @Override
    public WhiteboardApiGrpc.WhiteboardApiBlockingStub client() {
        init();
        return lzyWhiteboardClient;
    }

    @Override
    public void init() {
        if (lzyWhiteboardClient == null) {
            try {
                lzyWhiteboard = Utils.javaProcess(
                        WhiteboardApi.class.getCanonicalName(),
                        new String[]{
                                "--port",
                                String.valueOf(WHITEBOARD_PORT)
                        },
                        new String[]{
                                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32",
                        }
                ).inheritIO().start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            channel = ManagedChannelBuilder
                    .forAddress("localhost", WHITEBOARD_PORT)
                    .usePlaintext()
                    .build();
            lzyWhiteboardClient = WhiteboardApiGrpc.newBlockingStub(channel)
                    .withWaitForReady()
                    .withDeadlineAfter(WHITEBOARD_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);

            while (channel.getState(true) != ConnectivityState.READY) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }

    @Override
    public void close() {
        if (lzyWhiteboard != null) {
            try {
                channel.shutdown();
                channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                lzyWhiteboard.destroy();
                lzyWhiteboard.onExit().get(Long.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
