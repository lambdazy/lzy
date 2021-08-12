package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LzyServerProcessesContext implements LzyServerTestContext {
    private static final int LZY_SERVER_PORT = 7777;
    private Server lzyServer;
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
                lzyServer.shutdown();
                lzyServer.awaitTermination();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized void init() {
        if (lzyServer == null) {
            lzyServer = ServerBuilder.forPort(LZY_SERVER_PORT).addService(new LzyServer.Impl()).build();
            try {
                lzyServer.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            channel = ManagedChannelBuilder
                .forAddress("localhost", LZY_SERVER_PORT)
                .usePlaintext()
                .build();
            lzyServerClient = LzyServerGrpc.newBlockingStub(channel);
        }
    }

    public boolean waitForServants(long timeout, TimeUnit unit, int... ports) {
        init();
        final long finish = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);
        final Set<Integer> expected = Arrays.stream(ports).boxed().collect(Collectors.toSet());
        while (System.currentTimeMillis() < finish) {
            final Set<Integer> actual = lzyServerClient.servantsStatus(IAM.Auth.newBuilder()
                .setUser(IAM.UserCredentials.newBuilder()
                    .setUserId("uid")
                    .setToken("token")
                    .build()
                ).build()).getStatusesList()
                .stream()
                .map(value -> URI.create(value.getServantURI()).getPort())
                .collect(Collectors.toSet());
            if (expected.equals(actual)) {
                return true;
            } else {
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return false;
    }
}
