package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.io.IOException;
import java.net.URI;

public class LzySnapshot {
    private static final Logger LOG = LogManager.getLogger(LzySnapshot.class);

    private static final Options options = new Options();
    private static final String LZY_SNAPSHOT_HOST_ENV = "LZY_WHITEBOARD_HOST";
    private static final String DEFAULT_LZY_SNAPSHOT_LOCALHOST = "http://localhost";
    private static URI serverURI;

    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
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
        final int port = Integer.parseInt(parse.getOptionValue('p', "8999"));
        final String lzyWhiteboardHost;
        if (System.getenv().containsKey(LZY_SNAPSHOT_HOST_ENV)) {
            lzyWhiteboardHost = "http://" + System.getenv(LZY_SNAPSHOT_HOST_ENV);
        } else {
            lzyWhiteboardHost = DEFAULT_LZY_SNAPSHOT_LOCALHOST;
        }
        serverURI = URI.create(lzyWhiteboardHost + ":" + port);
        SnapshotApi spImpl = new SnapshotApi();
        final ManagedChannel snapshotChannel = ManagedChannelBuilder
                .forAddress(serverURI.getHost(), serverURI.getPort())
                .usePlaintext()
                .build();
        WhiteboardApi wbImpl = new WhiteboardApi(SnapshotApiGrpc.newBlockingStub(snapshotChannel));
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
