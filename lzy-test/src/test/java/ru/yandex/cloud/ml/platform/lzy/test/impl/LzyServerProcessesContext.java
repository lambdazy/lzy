package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.apache.commons.lang3.SystemUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

public class LzyServerProcessesContext implements LzyServerTestContext {

    private static final long SERVER_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_SERVER_PORT = 7777;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;
    private Process lzyServer;
    private ManagedChannel channel;

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
                ProcessBuilder builder = Utils.javaProcess(
                    LzyServer.class.getCanonicalName(),
                    new String[] {
                        "--port",
                        String.valueOf(LZY_SERVER_PORT)
                    },
                    new String[] {
                        "-Djava.util.concurrent.ForkJoinPool.common.parallelism=32",
                        "-Dlzy.server.task.type=local-docker"
                    }
                );
                Map<String, String> env = builder.environment();
                env.put("STORAGE_AMAZON_ACCESS_TOKEN", "access-key");
                env.put("STORAGE_AMAZON_SECRET_TOKEN", "secret-key");
                env.put("STORAGE_AMAZON_ENABLED", "true");
                env.put("STORAGE_BUCKET", "lzy-bucket");
                String serviceEndpoint;
                String lzywhiteboard;
                if (!SystemUtils.IS_OS_LINUX) {
                    serviceEndpoint = "http://host.docker.internal:8001";
                    lzywhiteboard = "http://host.docker.internal:8999";
                } else {
                    serviceEndpoint = "http://localhost:8001";
                    lzywhiteboard = "http://localhost:8999";
                }
                env.put("STORAGE_AMAZON_ENDPOINT", serviceEndpoint);
                env.put("LZYWHITEBOARD", lzywhiteboard);
                lzyServer = builder.inheritIO().start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            channel = ChannelBuilder
                .forAddress("localhost", LZY_SERVER_PORT)
                .usePlaintext()
                .enableRetry(LzyServerGrpc.SERVICE_NAME)
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
