package ai.lzy.kafka;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.util.grpc.GrpcHeadersServerInterceptor;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.util.grpc.RequestIdInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

@Singleton
public class Main {
    private final Server server;

    public Main(@Named("S3SinkIamChannel") ManagedChannel iamChannel, SinkServiceImpl sinkService,
                ServiceConfig config)
    {

        var hostAndPort = HostAndPort.fromString(config.getAddress());

        var authInterceptor = new AuthServerInterceptor(new AuthenticateServiceGrpcClient("LzyService", iamChannel));

        server = NettyServerBuilder.forAddress(new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(500, TimeUnit.MILLISECONDS)
            .keepAliveTime(1000, TimeUnit.MILLISECONDS)
            .keepAliveTimeout(500, TimeUnit.MILLISECONDS)
            .intercept(authInterceptor)
            .intercept(GrpcLogsInterceptor.server())
            .intercept(RequestIdInterceptor.server(true))
            .intercept(GrpcHeadersServerInterceptor.create())
            .addService(sinkService)
            .build();

        try {
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @PreDestroy
    public void close() {
        server.shutdownNow();
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            // ignored
        }
    }

    public static void main(String[] args) {
        var context = ApplicationContext.run();
        var main = context.getBean(Main.class);

        try {
            main.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
