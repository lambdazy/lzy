package ai.lzy.channelmanager;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.services.ChannelService;
import ai.lzy.channelmanager.services.SlotsService;
import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.runtime.Micronaut;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.SERVICE_NAME;

@Singleton
public class ChannelManagerMain {
    public static final String APP = "ChannelManager";

    private static final Logger LOG = LogManager.getLogger(ChannelManagerMain.class);

    private final Server server;
    private final ActionScheduler action;

    public ChannelManagerMain(
        ChannelManagerConfig config,
        ChannelService channelService, SlotsService slotsService,
        @Named("ChannelManagerIamGrpcChannel") ManagedChannel iamChannel, ActionScheduler action)
    {
        this.action = action;
        LOG.info("Starting ChannelManager service with config: {}", config);

        final var authInterceptor = new AuthServerInterceptor(
            new AuthenticateServiceGrpcClient(SERVICE_NAME, iamChannel));

        final var channelManagerAddress = HostAndPort.fromString(config.getAddress());

        final var internalOnly = new AccessServerInterceptor(
            new AccessServiceGrpcClient(SERVICE_NAME, iamChannel),
            config.getIam().createRenewableToken()::get, Root.INSTANCE, AuthPermission.INTERNAL_AUTHORIZE);

        this.server = newGrpcServer(channelManagerAddress, authInterceptor)
            .addService(slotsService)
            .addService(ServerInterceptors.intercept(channelService, internalOnly))
            .build();
    }

    public void start() {
        try {
            server.start();
            action.restoreActions();
            LOG.info("Channel manager started, listening on {}", server.getPort());
        } catch (IOException e) {
            LOG.error("Cannot start channel manager", e);
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        server.shutdown();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @PreDestroy
    public void close() throws InterruptedException {
        stop();
        awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var context = Micronaut.run(ChannelManagerMain.class, args)) {
            final var channelManagerApp = context.getBean(ChannelManagerMain.class);
            channelManagerApp.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Shutting down channel manager");
                channelManagerApp.stop();
            }));

            channelManagerApp.awaitTermination();
        }
    }
}
