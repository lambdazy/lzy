package ai.lzy.storage;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.LzyStorageServiceGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
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

    private final ManagedChannel iamChannel;
    private final Server server;

    public LzyStorage(ApplicationContext context) {
        var config = context.getBean(StorageConfig.class);

        var address = HostAndPort.fromString(config.getAddress());
        var iamAddress = HostAndPort.fromString(config.getIam().getAddress());

        var service = context.getBean(LzyStorageServiceGrpc.LzyStorageServiceImplBase.class);

        iamChannel = ChannelBuilder.forAddress(iamAddress)
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();

        server = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(iamChannel)))
            .addService(ServerInterceptors.intercept(service, new AllowInternalUserOnlyInterceptor(iamChannel)))
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
