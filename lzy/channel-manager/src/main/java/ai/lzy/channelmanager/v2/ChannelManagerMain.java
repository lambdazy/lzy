package ai.lzy.channelmanager.v2;

import ai.lzy.channelmanager.ChannelManagerApp;
import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.v2.services.ChannelService;
import ai.lzy.channelmanager.v2.services.SlotsService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;
import static ai.lzy.v1.channel.v2.LzyChannelManagerPrivateGrpc.SERVICE_NAME;

@Singleton
public class ChannelManagerMain {
    private static final Logger LOG = LogManager.getLogger(ChannelManagerMain.class);

    private final Server server;
    private final StartTransferAction action;

    public ChannelManagerMain(
        ChannelManagerConfig config,
        ChannelService channelService, SlotsService slotsService,
        @Named("ChannelManagerIamGrpcChannel") ManagedChannel iamChannel, StartTransferAction action)
    {
        this.action = action;
        LOG.info("Starting ChannelManager service with config: {}", config);

        final var authInterceptor = new AuthServerInterceptor(
            new AuthenticateServiceGrpcClient(SERVICE_NAME, iamChannel));

        final var channelManagerAddress = HostAndPort.fromString(config.getAddress());

        final var internalOnly = new AllowInternalUserOnlyInterceptor(SERVICE_NAME, iamChannel);

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

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var context = Micronaut.run(ChannelManagerApp.class, args)) {
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
