package ai.lzy.storage;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.runtime.Micronaut;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    public static final String APP = "LzyStorage";

    private final ManagedChannel iamChannel;
    private final Server server;

    private final ExecutorService workersPool;

    public App(StorageConfig config, StorageServiceGrpc service,
               @Named("StorageOperationDao") OperationDao operationDao,
               @Named("StorageServiceServerExecutor") ExecutorService workersPool)
    {
        var address = HostAndPort.fromString(config.getAddress());
        this.iamChannel = newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var authInterceptor = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));
        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        var operationService = new OperationsService(operationDao);
        this.workersPool = workersPool;

        server = createServer(
            address,
            authInterceptor,
            new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
                    return workersPool;
                }
            },
            ServerInterceptors.intercept(operationService, internalOnly),
            ServerInterceptors.intercept(service, internalOnly));
    }

    public void start() throws IOException {
        server.start();
    }

    public void close(boolean force) {
        if (force) {
            server.shutdownNow();
            workersPool.shutdownNow();
            iamChannel.shutdownNow();
        } else {
            server.shutdown();
            workersPool.shutdown();
            iamChannel.shutdown();
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
        try {
            //noinspection ResultOfMethodCallIgnored
            workersPool.awaitTermination(10, TimeUnit.SECONDS);
        } finally {
            iamChannel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public static Server createServer(HostAndPort address, AuthServerInterceptor authInterceptor,
                                      ServerServiceDefinition... services)
    {
        return createServer(address, authInterceptor, new ServerCallExecutorSupplier() {
            @Nullable
            @Override
            public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
                return null;
            }
        }, services);
    }

    public static Server createServer(HostAndPort address, AuthServerInterceptor authInterceptor,
                                      ServerCallExecutorSupplier executorSupplier, ServerServiceDefinition... services)
    {
        var serverBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(authInterceptor)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.forward())
            .intercept(GrpcHeadersServerInterceptor.create())
            .addServices(Arrays.asList(services));

        return serverBuilder.callExecutor(executorSupplier).build();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (var context = Micronaut.run(App.class, args)) {
            var main = context.getBean(App.class);
            main.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Storage gRPC server is shutting down!");
                main.close(false);
            }));
            main.awaitTermination();
        }
    }
}
