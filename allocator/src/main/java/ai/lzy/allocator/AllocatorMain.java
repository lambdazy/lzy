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
import java.util.concurrent.TimeUnit;

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

        ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
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

    private void start() throws IOException {
        LOG.info("Starting allocator on port {}...", config.port());
        server.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final var context = ApplicationContext.run();
        final var main = context.getBean(AllocatorMain.class);
        main.start();
        final var thread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping allocator service");
            main.server.shutdown();
            main.gc.shutdown();
            while (true) {
                try {
                    thread.join();
                    break;
                } catch (InterruptedException e) {
                    LOG.debug(e);
                }
            }
        }));
        main.server.awaitTermination();
    }
}
