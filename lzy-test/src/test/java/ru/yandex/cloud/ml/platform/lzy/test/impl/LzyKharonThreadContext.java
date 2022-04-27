package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.kharon.LzyKharon;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LzyKharonThreadContext implements LzyKharonTestContext {

    private static final long KHARON_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_KHARON_PORT = 8899;
    private static final int LZY_KHARON_SERVANT_PROXY_PORT = 8900;

    private final String serverAddress;
    private final String whiteboardAddress;
    private LzyKharon kharon;
    private ManagedChannel channel;
    private LzyKharonGrpc.LzyKharonBlockingStub lzyKharonClient;

    public LzyKharonThreadContext(String serverAddress, String whiteboardAddress) {
        this.serverAddress = serverAddress;
        this.whiteboardAddress = whiteboardAddress;
    }

    @Override
    public String serverAddress(boolean fromDocker) {
        return "http://" + outerHost(fromDocker) + ":" + LZY_KHARON_PORT;
    }

    @Override
    public String servantAddress(boolean fromDocker) {
        return "http://" + outerHost(fromDocker) + ":" + LZY_KHARON_SERVANT_PROXY_PORT;
    }

    @Override
    public LzyKharonGrpc.LzyKharonBlockingStub client() {
        return lzyKharonClient;
    }

    @Override
    public void init() {
        try {
            kharon = new LzyKharon(
                URI.create(serverAddress),
                URI.create(whiteboardAddress),
                URI.create(whiteboardAddress),
                outerHost(false),
                LZY_KHARON_PORT,
                LZY_KHARON_SERVANT_PROXY_PORT,
                outerHost(false)
            );
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

    private String outerHost(boolean fromDocker) {
        if (!SystemUtils.IS_OS_LINUX && fromDocker) {
            return "host.docker.internal";
        } else {
            return "localhost";
        }
    }
}
