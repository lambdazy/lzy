package ai.lzy.kafka.s3sink;

import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.util.grpc.GrpcUtils;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
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
    private static final String APP = "S3Sink";

    private final Server server;

    public Main(@Named("S3SinkIamChannel") ManagedChannel iamChannel, SinkServiceImpl sinkService,
                ServiceConfig config)
    {

        var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));
        var internalOnly = new AccessServerInterceptor(
            new AccessServiceGrpcClient(APP, iamChannel),
            config.getIam().createRenewableToken()::get, Root.INSTANCE, AuthPermission.INTERNAL_AUTHORIZE);

        server = GrpcUtils.newGrpcServer(HostAndPort.fromString(config.getAddress()), auth)
            .addService(ServerInterceptors.intercept(sinkService, internalOnly))
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
