package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.kharon.Kharon;
import ru.yandex.cloud.ml.platform.lzy.test.LzyKharonTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LzyKharonProcessesContext implements LzyKharonTestContext {
    private static final long KHARON_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_KHARON_PORT = 8899;
    private Process lzyKharon;
    private ManagedChannel channel;
    protected LzyKharonGrpc.LzyKharonBlockingStub lzyKharonClient;

    @Override
    public String host(boolean fromDocker) {
        if (!SystemUtils.IS_OS_LINUX && fromDocker) {
            return "host.docker.internal";
        } else {
            return "localhost";
        }
    }

    @Override
    public int port() {
        init();
        return LZY_KHARON_PORT;
    }

    @Override
    public LzyKharonGrpc.LzyKharonBlockingStub client() {
        init();
        return lzyKharonClient;
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

    private synchronized void init() {
        if (lzyKharonClient == null) {
            try {
                lzyKharon = Utils.javaProcess(
                    Kharon.class.getCanonicalName(),
                    new String[]{
                        "--port",
                        String.valueOf(LZY_KHARON_PORT)
                    },
                    new String[]{}
                ).inheritIO().start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            channel = ManagedChannelBuilder
                .forAddress("localhost", LZY_KHARON_PORT)
                .usePlaintext()
                .build();
            lzyKharonClient = LzyKharonGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(KHARON_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);
        }
    }
}
