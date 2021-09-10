package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class LzyServerProcessesContext implements LzyServerTestContext {
    private static final long SERVER_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_SERVER_PORT = 7777;
    private Process lzyServer;
    private ManagedChannel channel;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;

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
        return LZY_SERVER_PORT;
    }

    @Override
    public LzyServerGrpc.LzyServerBlockingStub client() {
        init();
        return lzyServerClient;
    }

    @Override
    public synchronized void close() {
        if (lzyServer != null) {
            try {
                channel.shutdown();
                channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                lzyServer.destroy();
                lzyServer.onExit().get(Long.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized void init() {
        if (lzyServer == null) {
            try {
                lzyServer = Utils.javaProcess(
                    LzyServer.class.getCanonicalName(),
                    new String[]{
                        "--port",
                        String.valueOf(LZY_SERVER_PORT)
                    }
                ).inheritIO().start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            channel = ManagedChannelBuilder
                .forAddress("localhost", LZY_SERVER_PORT)
                .usePlaintext()
                .build();
            lzyServerClient = LzyServerGrpc.newBlockingStub(channel)
                .withWaitForReady()
                .withDeadlineAfter(SERVER_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);
        }
    }

    public boolean waitForServants(long timeout, TimeUnit unit, int... ports) {
        init();
        final Set<Integer> expected = Arrays.stream(ports).boxed().collect(Collectors.toSet());
        return true;
        //return Utils.waitFlagUp(() -> {
        //    final Set<Integer> actual = lzyServerClient.servantsStatus(IAM.Auth.newBuilder()
        //        .setUser(IAM.UserCredentials.newBuilder()
        //            .setUserId("uid")
        //            .setToken("token")
        //            .build()
        //        ).build()).getStatusesList()
        //        .stream()
        //        .map(value -> URI.create(value.getServantURI()).getPort())
        //        .collect(Collectors.toSet());
        //    return expected.equals(actual);
        //}, timeout, unit);
    }
}
