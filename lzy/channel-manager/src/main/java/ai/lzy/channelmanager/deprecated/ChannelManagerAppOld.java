package ai.lzy.channelmanager.deprecated;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.deprecated.grpc.ChannelManagerPrivateService;
import ai.lzy.channelmanager.deprecated.grpc.ChannelManagerService;
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
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManagerAppOld {

    private static final Logger LOG = LogManager.getLogger(ChannelManagerAppOld.class);
    private static final Options options = new Options();

    public static final String APP = "LzyChannelManager";

    static {
        options.addRequiredOption("p", "port", true, "gRPC port setting");
        options.addRequiredOption("w", "lzy-whiteboard-address", true, "Lzy whiteboard address [host:port]");
    }

    private final Server channelManagerServer;
    private final ManagedChannel iamChannel;

    public static void main(String[] args) throws IOException, InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        CommandLine parse = null;
        try {
            parse = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("channel-manager", options);
            System.exit(-1);
        }
        final URI address = URI.create(parse.getOptionValue('a', "localhost:8122"));
        final URI whiteboardAddress = URI.create(parse.getOptionValue('w', "http://localhost:8999"));

        try (ApplicationContext context = ApplicationContext.run(Map.of(
            "channel-manager.address", address,
            "channel-manager.whiteboard-address", whiteboardAddress.toString()
        )))
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
