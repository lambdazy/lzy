package ai.lzy.storage;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.util.grpc.*;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    public static final String APP = "LzyStorage";

    private final ManagedChannel iamChannel;
    private final Server server;

    public App(ApplicationContext context) {
        var config = context.getBean(StorageConfig.class);

        var address = HostAndPort.fromString(config.getAddress());
        var iamAddress = HostAndPort.fromString(config.getIam().getAddress());

        var service = context.getBean(LzyStorageServiceGrpc.LzyStorageServiceImplBase.class);
        var operationDao = context.getBean(OperationDao.class, Qualifiers.byName(BeanFactory.DAO_NAME));
        var opService = new OperationService(operationDao);

        iamChannel = GrpcUtils.newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);
        var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        server = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server())
            .intercept(GrpcHeadersServerInterceptor.create())
            .addService(ServerInterceptors.intercept(opService, internalOnly))
            .addService(ServerInterceptors.intercept(service, internalOnly))
            .build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            try {
                server.shutdown();
            } finally {
                iamChannel.shutdown();
            }
        }));
    }

    public void close(boolean force) {
        try {
            if (force) {
                server.shutdownNow();
            } else {
                server.shutdown();
            }
        } finally {
            if (force) {
                iamChannel.shutdownNow();
            } else {
                iamChannel.shutdown();
            }
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
        iamChannel.awaitTermination(10, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run("storage")) {
            var app = new App(context);

            app.start();
            app.awaitTermination();
        } catch (NoSuchBeanException e) {
            LOG.fatal(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
