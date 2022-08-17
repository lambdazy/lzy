package ai.lzy.allocator;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.services.AllocatorApi;
import ai.lzy.allocator.services.AllocatorPrivateApi;
import ai.lzy.allocator.services.OperationApi;
import ai.lzy.allocator.vmpool.VmPoolService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.GrpcLogsInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Named;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class AllocatorMain {

    private static final Logger LOG = LogManager.getLogger(AllocatorMain.class);
    private final ServiceConfig config;
    private final Server server;
    private final GarbageCollector gc;

    public AllocatorMain(AllocatorApi allocator, AllocatorPrivateApi allocatorPrivate, OperationApi opApi,
                         ServiceConfig config, GarbageCollector gc, VmPoolService vmPool,
                         @Named("IamGrpcChannel") ManagedChannel iamChannel) {
        this.config = config;
        this.gc = gc;

        final HostAndPort address = HostAndPort.fromString(config.getAddress());
        ServerBuilder<?> builder = NettyServerBuilder.forAddress(
                new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

        // TODO: X-REQUEST-ID header (and others) interceptor(s)
        builder.intercept(new GrpcLogsInterceptor());
        builder.intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(iamChannel)));

        var internalOnly = new AllowInternalUserOnlyInterceptor(iamChannel);

        builder.addService(ServerInterceptors.intercept(allocator, internalOnly));
        builder.addService(allocatorPrivate);
        builder.addService(ServerInterceptors.intercept(opApi, internalOnly));
        builder.addService(ServerInterceptors.intercept(vmPool, internalOnly));

        server = builder.build();
    }

    public void start() throws IOException {
        LOG.info("Starting allocator at {}...", config.getAddress());
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
        var config = context.getBean(ServiceConfig.class);
        final var main = context.getBean(AllocatorMain.class);
        main.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping allocator service");
            main.stop();
        }));
        main.awaitTermination();
    }
}
