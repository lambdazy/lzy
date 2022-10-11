package ai.lzy.whiteboard;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import ai.lzy.whiteboard.grpc.WhiteboardPrivateService;
import ai.lzy.whiteboard.grpc.WhiteboardService;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class WhiteboardApp {
    private static final Logger LOG = LogManager.getLogger(WhiteboardApp.class);

    public static final String APP = "LzyWhiteboard";

    private final Server whiteboardServer;

    public WhiteboardApp(AppConfig config, @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel,
                         WhiteboardService whiteboardService, WhiteboardPrivateService whiteboardPrivateService)
    {
        final HostAndPort address = HostAndPort.fromString(config.getAddress());
        final var internalUserOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        whiteboardServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server())
            .intercept(GrpcHeadersServerInterceptor.create())
            .addService(whiteboardService)
            .addService(ServerInterceptors.intercept(whiteboardPrivateService, internalUserOnly))
            .build();
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

    public void start() throws IOException {
        whiteboardServer.start();
        LOG.info("Whiteboard server started on {}",
            whiteboardServer.getListenSockets().stream().map(Object::toString).collect(Collectors.joining())
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            stop();
        }));
    }

    public void stop() {
        whiteboardServer.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        whiteboardServer.awaitTermination();
    }
}
