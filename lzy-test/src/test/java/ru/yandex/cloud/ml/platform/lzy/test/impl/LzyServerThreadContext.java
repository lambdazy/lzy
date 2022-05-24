package ru.yandex.cloud.ml.platform.lzy.test.impl;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.logging.log4j.LogManager;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LzyServerThreadContext implements LzyServerTestContext {

    private static final long SERVER_STARTUP_TIMEOUT_SEC = 60;
    private static final int LZY_SERVER_PORT = 7777;
    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;
    private Server lzyServer;
    private ManagedChannel channel;
    private LzyServer.Impl impl;

    @Override
    public String address() {
        return "http://localhost:" + LZY_SERVER_PORT;
    }

    @Override
    public LzyServerGrpc.LzyServerBlockingStub client() {
        return lzyServerClient;
    }

    @Override
    public void init() {
        final String serviceEndpoint = "http://localhost:8001";
        final String lzyWhiteboard = "http://localhost:8999";

        try (ApplicationContext context = ApplicationContext.run(
            PropertySource.of(
                Map.of(
                    "server.server-uri", address(),
                    "storage.amazon.endpoint", serviceEndpoint,
                    "server.whiteboardUri", lzyWhiteboard,
                    "storage.amazon.enabled", "true",
                    "storage.bucket", "lzy-bucket",
                    "storage.amazon.accessToken", "access-key",
                    "storage.amazon.secretToken", "secret-key",
                    "server.threadAllocator.enabled", "true",
                    "server.threadAllocator.filePath", "lzy-servant/target/classes/ru/yandex/cloud/ml/platform/lzy/servant/BashApi.class",
                    "servant.dockerSupport.enabled", "false"
                )
            )
        )) {
            var logger = LogManager.getLogger(LzyServer.class);
            logger.info("Starting LzyServer on port {}...", LZY_SERVER_PORT);

            impl = context.getBean(LzyServer.Impl.class);
            ServerBuilder<?> builder = NettyServerBuilder.forPort(LZY_SERVER_PORT)
                    .permitKeepAliveWithoutCalls(true)
                    .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
                    .addService(impl);
            lzyServer = builder.build();
            lzyServer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("gRPC server is shutting down!");
                lzyServer.shutdown();
            }));
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (lzyServer != null) {
            try {
                channel.shutdownNow();
                channel.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                lzyServer.shutdownNow();
                lzyServer.awaitTermination();
                impl.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
