package ai.lzy.kafka.s3sink;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.util.grpc.GrpcUtils;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

@Singleton
public class Main {
    private static final Logger LOG = LogManager.getLogger(Main.class);

    private final Server server;

    public Main(@Named("S3SinkIamChannel") ManagedChannel iamChannel, SinkServiceImpl sinkService,
                ServiceConfig config)
    {

        var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient("S3Sink", iamChannel));

        server = GrpcUtils.newGrpcServer(HostAndPort.fromString(config.getAddress()), auth)
            .addService(sinkService)
            .build();

        try {
            server.start();
            LOG.info("S3-sink started on {}", config.getAddress());
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
