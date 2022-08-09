package ai.lzy.test.impl;

import ai.lzy.servant.env.EnvironmentFactory;
import ai.lzy.test.LzyServerTestContext;
import ai.lzy.model.grpc.ChannelBuilder;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.LogManager;
import ai.lzy.server.LzyServer;
import ai.lzy.v1.LzyServerGrpc;

public class ServerThreadContext implements LzyServerTestContext {

    static class Config extends Utils.Defaults {
        private static final long SERVER_STARTUP_TIMEOUT_SEC = 60;
        private static final int SERVER_PORT = 7777;
    }

    protected LzyServerGrpc.LzyServerBlockingStub lzyServerClient;
    private final Map<String, Object> appProperties = new HashMap<>();
    private Server lzyServer;
    private ManagedChannel channel;
    private LzyServer.Impl impl;

    public ServerThreadContext(LocalServantAllocatorType allocatorType) {
        super();
        appProperties.putAll(Map.of(
            "server.server-uri", "http://localhost:" + Config.SERVER_PORT,
            "server.channel-manager-uri", "http://localhost:" + Config.CHANNEL_MANAGER_PORT,
            "storage.amazon.endpoint", "http://localhost:" + Config.S3_PORT,
            "server.whiteboardUri", "http://localhost:" + Config.WHITEBOARD_PORT,
            "storage.amazon.enabled", "true",
            "storage.bucket", "lzy-bucket",
            "storage.amazon.accessToken", "access-key",
            "storage.amazon.secretToken", "secret-key"
        ));
        switch (allocatorType) {
            case THREAD_ALLOCATOR -> {
                appProperties.putAll(Map.of(
                    "server.threadAllocator.enabled", "true",
                    "server.threadAllocator.filePath", "servant/target/classes/ai/lzy/servant/BashApi.class"
                ));
                EnvironmentFactory.disableDockers();
            }
            case DOCKER_ALLOCATOR -> {
                final String envImage = System.getProperty("server.baseEnvDefaultImage", "lzydock/test-env:master");
                appProperties.putAll(Map.of(
                    "server.dockerAllocator.enabled", "true",
                    "server.baseEnvDefaultImage", envImage
                ));
            }
        }
    }

    @Override
    public String address() {
        return "http://localhost:" + Config.SERVER_PORT;
    }

    @Override
    public LzyServerGrpc.LzyServerBlockingStub client() {
        return lzyServerClient;
    }

    @Override
    public void init() {
        try (ApplicationContext context = ApplicationContext.run(PropertySource.of(appProperties))) {
            var logger = LogManager.getLogger(LzyServer.class);
            logger.info("Starting LzyServer on port {}...", Config.SERVER_PORT);

            impl = context.getBean(LzyServer.Impl.class);
            ServerBuilder<?> builder = NettyServerBuilder.forPort(Config.SERVER_PORT)
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
                    .forAddress("localhost", Config.SERVER_PORT)
                    .usePlaintext()
                    .enableRetry(LzyServerGrpc.SERVICE_NAME)
                    .build();
            lzyServerClient = LzyServerGrpc.newBlockingStub(channel)
                    .withWaitForReady()
                    .withDeadlineAfter(Config.SERVER_STARTUP_TIMEOUT_SEC, TimeUnit.SECONDS);
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
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
