package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.cli.*;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.mem.InMemRepo;

import java.io.IOException;

public class LzySnapshot {
    private static final Options options = new Options();

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
        InMemRepo repo = new InMemRepo();
        SnapshotApi spImpl = new SnapshotApi(repo);
        WhiteboardApi wbImpl = new WhiteboardApi(repo, repo);
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
