package ai.lzy.channelmanager;

import ai.lzy.channelmanager.v2.grpc.ChannelManagerPrivateService;
import ai.lzy.channelmanager.v2.grpc.ChannelManagerService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.OperationService;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.micronaut.runtime.Micronaut;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Singleton
public class ChannelManagerApp {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerApp.class);

    public static final String APP = "ChannelManager";

    private final Server channelManagerServer;
    private final ManagedChannel iamChannel;

    public ChannelManagerApp(ChannelManagerConfig config,
                             ChannelManagerService channelManagerService,
                             ChannelManagerPrivateService channelManagerPrivateService,
                             OperationService operationService)
    {
        LOG.info("Starting ChannelManager service with config: {}", config.toString());

        final var iamAddress = HostAndPort.fromString(config.getIam().getAddress());
        final var chanelManagerAddress = HostAndPort.fromString(config.getAddress());

        iamChannel = newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);
        final var authInterceptor = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel));

        channelManagerServer = newGrpcServer(chanelManagerAddress, authInterceptor)
            .addService(channelManagerService)
            .addService(channelManagerPrivateService)
            .addService(operationService)
            .build();
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

        restoreActiveOperations();

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
