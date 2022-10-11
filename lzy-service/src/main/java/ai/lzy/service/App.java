package ai.lzy.service;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.runtime.Micronaut;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    private final Server server;

    public App(Server server) {
        this.server = server;
    }

    public void start() throws IOException {
        server.start();
    }

    public void shutdown(boolean force) {
        if (force) {
            server.shutdownNow();
        } else {
            server.shutdown();
        }
    }

    private void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static Server createServer(HostAndPort endpoint, AuthServerInterceptor authInterceptor, LzyService service) {
        return NettyServerBuilder
            .forAddress(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(authInterceptor)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server())
            .intercept(GrpcHeadersServerInterceptor.create())
            .addService(service)
            .build();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var context = Micronaut.run(App.class, args)) {
            var config = context.getBean(LzyServiceConfig.class);

            var authInterceptor = new AuthServerInterceptor(
                new AuthenticateServiceGrpcClient("LzyService", config.getIam().getAddress()));

            var server = createServer(
                HostAndPort.fromString(config.getAddress()),
                authInterceptor,
                context.getBean(LzyService.class));

            var main = new App(server);
            main.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping lzy service");
                main.shutdown(false);
            }));
            main.awaitTermination();
        }
    }
}
