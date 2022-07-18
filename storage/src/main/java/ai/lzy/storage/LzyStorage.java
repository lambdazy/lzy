package ai.lzy.storage;

import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v2.LzyStorageGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
public class LzyStorage {
    private static final Logger LOG = LogManager.getLogger(LzyStorage.class);

    private final Server server;

    public LzyStorage(ApplicationContext context) {
        var config = context.getBean(StorageConfig.class);
        var address = HostAndPort.fromString(config.address());

        var service = context.getBean(LzyStorageGrpc.LzyStorageImplBase.class);

        server = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(service)
            .build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            server.shutdown();
        }));
    }

    public void close() {
        server.shutdownNow();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            var app = new LzyStorage(context);

            app.start();
            app.awaitTermination();
        } catch (NoSuchBeanException e) {
            LOG.fatal(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
