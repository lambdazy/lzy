package ai.lzy.whiteboard;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import ai.lzy.whiteboard.grpc.WhiteboardService;
import com.google.common.net.HostAndPort;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

@Singleton
public class WhiteboardApp {
    private static final Logger LOG = LogManager.getLogger(WhiteboardApp.class);

    public static final String APP = "LzyWhiteboard";

    private final ManagedChannel iamChannel;
    private final ExecutorService workersPool;
    private final Server whiteboardServer;

    public WhiteboardApp(AppConfig config,
                         @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel,
                         @Named("WhiteboardServiceServerExecutor") ExecutorService workersPool,
                         WhiteboardService whiteboardService)
    {
        var address = HostAndPort.fromString(config.getAddress());

        this.iamChannel = iamChannel;
        this.workersPool = workersPool;

        whiteboardServer = createServer(
            address,
            iamChannel,
            new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
                    return workersPool;
                }
            }, whiteboardService);
    }

    public void start() throws IOException {
        whiteboardServer.start();
        LOG.info("Whiteboard server started on {}",
            whiteboardServer.getListenSockets().stream().map(Object::toString).collect(Collectors.joining())
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("WB gRPC server is shutting down!");
            stop();
        }));
    }

    public void stop() {
        whiteboardServer.shutdown();
        workersPool.shutdown();
        iamChannel.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        whiteboardServer.awaitTermination();
        try {
            //noinspection ResultOfMethodCallIgnored
            workersPool.awaitTermination(10, TimeUnit.SECONDS);
        } finally {
            iamChannel.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public static Server createServer(HostAndPort address, ManagedChannel iamChannel, BindableService... services) {
        return createServer(address, iamChannel, new ServerCallExecutorSupplier() {
            @Nullable
            @Override
            public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
                return null;
            }
        }, services);
    }

    public static Server createServer(HostAndPort address, ManagedChannel iamChannel,
                                      ServerCallExecutorSupplier executorSupplier, BindableService... services)
    {
        var serverBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server())
            .intercept(GrpcHeadersServerInterceptor.create());

        for (var service : services) {
            serverBuilder.addService(service);
        }

        return serverBuilder.callExecutor(executorSupplier).build();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            var app = context.getBean(WhiteboardApp.class);

            app.start();
            app.awaitTermination();
        } catch (NoSuchBeanException e) {
            LOG.fatal(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
