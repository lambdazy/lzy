package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class LzyServerProcessesContext implements LzyServerTestContext {
    private static final long SERVER_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_SERVER_PORT = 7777;
    private final TaskType type;
    private Process lzyServer;
    private ManagedChannel channel;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;

    public LzyServerProcessesContext(TaskType type) {
        this.type = type;
    }

    @Override
    public String address(boolean fromDocker) {
        init();
        if (!SystemUtils.IS_OS_LINUX && fromDocker) {
            return "http://host.docker.internal:" + LZY_SERVER_PORT;
        } else {
            return "http://localhost:" + LZY_SERVER_PORT;
        }
    }

    @Override
    public TaskType type() {
        return type;
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

    public synchronized void init() {
        if (lzyServerClient == null) {
            try {
                lzyServer = Utils.javaProcess(
                    LzyServer.class.getCanonicalName(),
                    new String[]{
                        "--port",
                        String.valueOf(LZY_SERVER_PORT)
                    },
                    new String[]{
                        "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32",
                        "-Dtasks.taskType=" + type.toString()
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

            while (channel.getState(true) != ConnectivityState.READY) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }
}
