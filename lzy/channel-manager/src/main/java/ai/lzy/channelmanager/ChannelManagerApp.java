package ai.lzy.channelmanager;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.operation.ChannelOperationManager;
import ai.lzy.channelmanager.services.ChannelManagerPrivateService;
import ai.lzy.channelmanager.services.ChannelManagerService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationsService;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class ChannelManagerApp {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerApp.class);

    public static final String APP = "ChannelManager";

    private final Server channelManagerServer;
    private final ManagedChannel iamChannel;

    private final ChannelOperationManager channelOperationManager;
    private final ChannelManagerConfig config;

    public ChannelManagerApp(ChannelManagerConfig config,
                             @Named("ChannelManagerIamGrpcChannel") ManagedChannel iamChannel,
                             ChannelManagerService channelManagerService,
                             ChannelManagerPrivateService channelManagerPrivateService,
                             @Named("ChannelManagerOperationService") OperationsService operationsService,
                             ChannelOperationManager channelOperationManager)
    {
        this.config = config;
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
        LOG.info("Starting ChannelManager service with config: {}", config.toString());

        channelManagerServer.start();

        channelOperationManager.restoreActiveOperations();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping ChannelManager service");
            shutdown();
        }));
    }

    public boolean awaitTermination() throws InterruptedException {
        return awaitTermination(10, TimeUnit.SECONDS);
    }

    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
        var serverStopped = channelManagerServer.awaitTermination(timeout / 2, timeUnit);
        return iamChannel.awaitTermination(timeout / 2, timeUnit) && serverStopped;
    }

    public void shutdown() {
        channelManagerServer.shutdown();
        iamChannel.shutdown();
    }

    public void shutdownNow() {
        channelManagerServer.shutdownNow();
        iamChannel.shutdownNow();
    }

    @PreDestroy
    public void stop() {
        LOG.debug("Stopping channel manager...");

        this.shutdown();
        try {
            if (!this.awaitTermination()) {
                this.shutdownNow();
            }
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            this.shutdownNow();
        }
    }

    @VisibleForTesting
    public ChannelOperationManager getChannelOperationManager() {
        return channelOperationManager;
    }
}
