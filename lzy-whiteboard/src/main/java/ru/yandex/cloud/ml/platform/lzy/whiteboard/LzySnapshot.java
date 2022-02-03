package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.api.SnapshotApi;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.api.WhiteboardApi;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class LzySnapshot {

    private static final Logger LOG = LogManager.getLogger(LzySnapshot.class);

    private static final Options options = new Options();
    private static final String LZY_SNAPSHOT_HOST_ENV = "LZY_WHITEBOARD_HOST";
    private static final String DEFAULT_LZY_SNAPSHOT_LOCALHOST = "http://localhost";

    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(
            new Option("z", "lzy-server-address", true, "Lzy server address [host:port]"));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        CommandLine parse = null;
        try {
            parse = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("lzy-snapshot", options);
            System.exit(-1);
        }
        URI serverAddress = URI.create(parse.getOptionValue('z', "http://localhost:8888"));
        final String lzyWhiteboardHost;
        if (System.getenv().containsKey(LZY_SNAPSHOT_HOST_ENV)) {
            lzyWhiteboardHost = "http://" + System.getenv(LZY_SNAPSHOT_HOST_ENV);
        } else {
            lzyWhiteboardHost = DEFAULT_LZY_SNAPSHOT_LOCALHOST;
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "8999"));
        URI snapshotURI = URI.create(lzyWhiteboardHost + ":" + port);
        LOG.info("Starting at: " + snapshotURI);
        try (ApplicationContext context = ApplicationContext.run(
            PropertySource.of(
                Map.of(
                    "snapshot.uri", snapshotURI.toString(),
                    "server.uri", serverAddress.toString()
                )
            )
        )) {
            SnapshotApi spImpl = context.getBean(SnapshotApi.class);
            WhiteboardApi wbImpl = context.getBean(WhiteboardApi.class);
            ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(wbImpl);
            builder.addService(spImpl);

            final Server server = builder.build();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("gRPC server is shutting down!");
                server.shutdown();
            }));
            server.awaitTermination();
        }
    }
}
