package ai.lzy.whiteboard;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.exceptions.NoSuchBeanException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

@SuppressWarnings("UnstableApiUsage")
public class LzyWhiteboard {

    public static final Logger LOG;

    static {
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyWhiteboard.class);
    }

    private final Server whiteboardServer;
    private final ManagedChannel iamChannel;

    public LzyWhiteboard(ApplicationContext context) {
        final var config = context.getBean(WhiteboardConfig.class);
        final HostAndPort address = HostAndPort.fromString(config.getAddress());
        final var whiteboardService = context.getBean(WhiteboardService.class);

        final HostAndPort iamAddress = HostAndPort.fromString(config.getIam().getAddress());
        iamChannel = ChannelBuilder.forAddress(iamAddress)
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
        final var internalUserOnlyInterceptor = new AllowInternalUserOnlyInterceptor(iamChannel);

        whiteboardServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(new AuthenticateServiceGrpcClient(iamChannel)))
            .intercept(new GrpcLogsInterceptor())
            .addService(ServerInterceptors.intercept(whiteboardService, internalUserOnlyInterceptor))
            .build();
    }

    public void start() throws IOException {
        whiteboardServer.start();
        LOG.info("Whiteboard server started on {}",
            whiteboardServer.getListenSockets().stream().map(Object::toString).collect(Collectors.joining())
        );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            close(false);
        }));
    }

    public void close(boolean force) {
        try {
            if (force) {
                whiteboardServer.shutdownNow();
            } else {
                whiteboardServer.shutdown();
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
        whiteboardServer.awaitTermination();
        iamChannel.awaitTermination(10, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            var app = new LzyWhiteboard(context);

            app.start();
            app.awaitTermination();
        } catch (NoSuchBeanException e) {
            LOG.fatal(e.getMessage(), e);
            System.exit(-1);
        }
    }
}
