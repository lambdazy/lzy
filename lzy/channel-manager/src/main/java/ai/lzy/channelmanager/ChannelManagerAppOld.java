package ai.lzy.channelmanager;

import ai.lzy.channelmanager.grpc.ChannelManagerPrivateService;
import ai.lzy.channelmanager.grpc.ChannelManagerService;
import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.iam.clients.stub.AuthenticateServiceStub;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.micronaut.context.ApplicationContext;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManagerAppOld {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerAppOld.class);
    private static final Options options = new Options();

    public static final String APP = "LzyChannelManager";

    private final Server channelManagerServer;
    private final ManagedChannel iamChannel;

    public static void main(String[] args) throws IOException, InterruptedException {

        try (ApplicationContext context = ApplicationContext.run())
        {
            final ChannelManagerAppOld app = new ChannelManagerAppOld(context);
            app.start();
            app.awaitTermination();
        }
    }

    public void start() throws IOException {
        channelManagerServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("gRPC server is shutting down!");
            stop();
        }));
    }

    public void awaitTermination() throws InterruptedException {
        channelManagerServer.awaitTermination();
        iamChannel.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void stop() {
        channelManagerServer.shutdownNow();
        iamChannel.shutdown();
    }

    public ChannelManagerAppOld(ApplicationContext ctx) {
        var config = ctx.getBean(ChannelManagerConfig.class);
        final var iamAddress = HostAndPort.fromString(config.getIam().getAddress());
        iamChannel = newGrpcChannel(iamAddress, LzyAuthenticateServiceGrpc.SERVICE_NAME);

        var builder = newGrpcServer(
            HostAndPort.fromString(config.getAddress()),
            new AuthServerInterceptor(config.isStubIam()
                ? new AuthenticateServiceStub()
                : new AuthenticateServiceGrpcClient(APP, iamChannel)));

        channelManagerServer = builder
            .addService(ctx.getBean(ChannelManagerService.class))
            .addService(ctx.getBean(ChannelManagerPrivateService.class))
            .build();
    }

}
