package ai.lzy.allocator;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.services.AllocatorApi;
import ai.lzy.allocator.services.AllocatorPrivateApi;
import ai.lzy.allocator.services.OperationApi;
import ai.lzy.allocator.vmpool.VmPoolService;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.GrpcLogsInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import java.net.InetSocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class AllocatorMain {

    private static final Logger LOG = LogManager.getLogger(AllocatorMain.class);
    private final ServiceConfig config;
    private final Server server;
    private final GarbageCollector gc;

    public AllocatorMain(AllocatorApi allocator, AllocatorPrivateApi allocatorPrivate, OperationApi opApi,
        ServiceConfig config, GarbageCollector gc, VmPoolService vmPool) {
        this.config = config;
        this.gc = gc;
        final HostAndPort address = HostAndPort.fromString(config.address());
        ServerBuilder<?> builder = NettyServerBuilder.forAddress(
                new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        builder.addService(ServerInterceptors.intercept(allocator, new GrpcLogsInterceptor()));
        builder.addService(ServerInterceptors.intercept(allocatorPrivate, new GrpcLogsInterceptor()));
        builder.addService(ServerInterceptors.intercept(opApi, new GrpcLogsInterceptor()));
        builder.addService(ServerInterceptors.intercept(vmPool, new GrpcLogsInterceptor()));
        server = builder.build();
    }

    public void start() throws IOException {
        LOG.info("Starting allocator at {}...", config.address());
        server.start();
    }

    public void stop() {
        server.shutdown();
        gc.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final var context = ApplicationContext.run();
        final var main = context.getBean(AllocatorMain.class);
        main.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping allocator service");
            main.stop();
        }));
        main.awaitTermination();
    }
}
