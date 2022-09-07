package ai.lzy.channelmanager;

import ai.lzy.channelmanager.grpc.ChannelManagerService;
import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.GrpcLogsInterceptor;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
public class ChannelManager {

    private static final Logger LOG = LogManager.getLogger(ChannelManager.class);
    private static final Options options = new Options();

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
            final ChannelManager channelManager = new ChannelManager(context);
            channelManager.start();
            channelManager.awaitTermination();
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

    public ChannelManager(ApplicationContext ctx) {
        var config = ctx.getBean(ChannelManagerConfig.class);
        final HostAndPort address = HostAndPort.fromString(config.getAddress());
        final var iamAddress = HostAndPort.fromString(config.getIam().getAddress());
        iamChannel = ChannelBuilder.forAddress(iamAddress)
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
        channelManagerServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(address.getHost(), address.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .intercept(new AuthServerInterceptor(ctx.getBean(AuthenticateService.class)))
            .intercept(new GrpcLogsInterceptor())
            .addService(ctx.getBean(ChannelManagerService.class))
            .build();
    }

}
