package ai.lzy.test.impl;

import ai.lzy.test.LzyServerTestContext;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.server.LzyServer;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

@Deprecated
public class ServerProcessContext implements LzyServerTestContext {

    private static final long SERVER_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_SERVER_PORT = 7777;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;
    private Process lzyServer;
    private ManagedChannel channel;

    @Override
    public String address() {
        init();
        return "http://localhost:" + LZY_SERVER_PORT;
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
                final String serviceEndpoint = "http://localhost:" + Utils.Defaults.S3_PORT;
                final String lzyWhiteboard = "http://localhost:8999" + Utils.Defaults.WHITEBOARD_PORT;
                env.put("STORAGE_AMAZON_ENDPOINT", serviceEndpoint);
                env.put("SERVER_WHITEBOARD_URL", lzyWhiteboard);
                env.put("BASE_ENV_DEFAULT_IMAGE", "fixed-image");
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
