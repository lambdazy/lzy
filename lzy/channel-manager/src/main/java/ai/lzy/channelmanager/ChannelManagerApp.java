package ai.lzy.channelmanager;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.operation.ChannelOperationManager;
import ai.lzy.channelmanager.services.ChannelManagerPrivateService;
import ai.lzy.channelmanager.services.ChannelManagerService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationsService;
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
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class ChannelManagerApp {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerApp.class);

    public static final String APP = "ChannelManager";

    private final Server channelManagerServer;
    private final ManagedChannel iamChannel;

    private final ChannelOperationManager channelOperationManager;

    public ChannelManagerApp(ChannelManagerConfig config,
                             @Named("ChannelManagerIamGrpcChannel") ManagedChannel iamChannel,
                             ChannelManagerService channelManagerService,
                             ChannelManagerPrivateService channelManagerPrivateService,
                             @Named("ChannelManagerOperationService") OperationsService operationsService,
                             ChannelOperationManager channelOperationManager)
    {
        LOG.info("Starting ChannelManager service with config: {}", config.toString());

        this.iamChannel = iamChannel;

        final var authInterceptor = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));
        final var channelManagerAddress = HostAndPort.fromString(config.getAddress());

        final var internalOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        this.channelManagerServer = newGrpcServer(channelManagerAddress, authInterceptor)
            .addService(channelManagerService)
            .addService(ServerInterceptors.intercept(channelManagerPrivateService, internalOnly))
            .addService(operationsService)
            .build();

        this.channelOperationManager = channelOperationManager;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (var context = Micronaut.run(ChannelManagerApp.class, args)) {
            final ChannelManagerApp channelManagerApp = context.getBean(ChannelManagerApp.class);
            channelManagerApp.start();
            channelManagerApp.awaitTermination();
        }
    }

    public void start() throws IOException {
        channelManagerServer.start();

        channelOperationManager.restoreActiveOperations();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping ChannelManager service");
            stop();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        channelManagerServer.awaitTermination();
        iamChannel.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void stop() {
        channelManagerServer.shutdown();
        iamChannel.shutdown();
    }

}
