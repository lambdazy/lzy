package ai.lzy.service;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

@Singleton
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    private final ExecutorService workersPool;
    private final Server server;

    public App(LzyServiceConfig config, LzyService lzyService,
               @Named("LzyServiceOperationDao") OperationDao operationDao,
               @Named("LzyServiceServerExecutor") ExecutorService workersPool)
    {
        var authInterceptor = new AuthServerInterceptor(
            new AuthenticateServiceGrpcClient("LzyService", config.getIam().getAddress()));

        var operationService = new OperationService(operationDao);

        this.workersPool = workersPool;

        server = createServer(
            HostAndPort.fromString(config.getAddress()),
            authInterceptor,
            new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
                    return workersPool;
                }
            },
            lzyService,
            operationService);
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown(boolean force) {
        if (force) {
            server.shutdownNow();
            workersPool.shutdownNow();
        } else {
            server.shutdown();
            workersPool.shutdown();
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
        //noinspection ResultOfMethodCallIgnored
        workersPool.awaitTermination(10, TimeUnit.SECONDS);
    }

    public static Server createServer(HostAndPort endpoint, AuthServerInterceptor authInterceptor,
                                      BindableService... services)
    {
        return createServer(endpoint, authInterceptor, new ServerCallExecutorSupplier() {
            @Nullable
            @Override
            public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
                return null;
            }
        }, services);
    }

    public static Server createServer(HostAndPort endpoint, AuthServerInterceptor authInterceptor,
                                      ServerCallExecutorSupplier executorSupplier, BindableService... services)
    {
        var serverBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(authInterceptor)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server(true))
            .intercept(GrpcHeadersServerInterceptor.create());

        for (var service : services) {
            serverBuilder.addService(service);
        }

        return serverBuilder.callExecutor(executorSupplier).build();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var context = Micronaut.run(App.class, args)) {
            var main = context.getBean(App.class);
            main.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping lzy service");
                main.shutdown(false);
            }));
            main.awaitTermination();
        }
    }
}
