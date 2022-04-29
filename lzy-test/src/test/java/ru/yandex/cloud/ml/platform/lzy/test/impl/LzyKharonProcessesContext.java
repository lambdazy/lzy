package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.LzyFsServer;
import ru.yandex.cloud.ml.platform.lzy.kharon.LzyKharon;
import ru.yandex.cloud.ml.platform.lzy.model.UriScheme;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

public class LzyKharonProcessesContext implements LzyKharonTestContext {

    private static final long KHARON_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_KHARON_PORT = 8899;
    private static final int LZY_KHARON_SERVANT_PROXY_PORT = 8900;

    private final String serverAddress;
    private final String whiteboardAddress;
    private Process lzyKharon;
    private ManagedChannel channel;
    private LzyKharonGrpc.LzyKharonBlockingStub lzyKharonClient;

    public LzyKharonProcessesContext(String serverAddress, String whiteboardAddress) {
        this.serverAddress = serverAddress;
        this.whiteboardAddress = whiteboardAddress;
    }

    @Override
    public String serverAddress(boolean fromDocker) {
        return "http://" + outerHost(fromDocker) + ":" + LZY_KHARON_PORT;
    }

    @Override
    public String servantAddress(boolean fromDocker) {
        return UriScheme.LzyServant.scheme() + outerHost(fromDocker) + ":" + LZY_KHARON_SERVANT_PROXY_PORT;
    }

    @Override
    public String servantFsAddress(boolean fromDocker) {
        return UriScheme.LzyFs.scheme() + outerHost(fromDocker) + ":" + LzyFsServer.DEFAULT_PORT;
    }

    @Override
    public LzyKharonGrpc.LzyKharonBlockingStub client() {
        return lzyKharonClient;
    }

    @Override
    public synchronized void init() {
        if (lzyKharonClient == null) {
            try {
                lzyKharon = Utils.javaProcess(
                    LzyKharon.class.getCanonicalName(),
                    new String[] {
                        "--host",
                        outerHost(true),
                        "--port",
                        String.valueOf(LZY_KHARON_PORT),
                        "--servant-proxy-port",
                        String.valueOf(LZY_KHARON_SERVANT_PROXY_PORT),
                        "--servantfs-proxy-port",
                        String.valueOf(LzyFsServer.DEFAULT_PORT),
                        "--lzy-server-address",
                        serverAddress,
                        "--lzy-whiteboard-address",
                        whiteboardAddress
                    },
                    new String[] {
                        "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32"
                    }
                ).inheritIO().start();
            } catch (IOException e) {
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
    public synchronized void close() {
        if (lzyKharon != null) {
            try {
                channel.shutdown();
                channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                lzyKharon.destroy();
                lzyKharon.onExit().get(Long.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String outerHost(boolean fromDocker) {
        if (!SystemUtils.IS_OS_LINUX && fromDocker) {
            return "host.docker.internal";
        } else {
            return "localhost";
        }
    }
}
