package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.exceptions.NoSuchBeanException;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.api.SnapshotApi;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.api.WhiteboardApi;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.mem.InMemRepo;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.mem.SnapshotRepositoryImpl;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.mem.WhiteboardRepositoryImpl;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class LzySnapshot {
    private static final Logger LOG = LogManager.getLogger(LzySnapshot.class);

    private static final Options options = new Options();
    private static final String LZY_SNAPSHOT_HOST_ENV = "LZY_WHITEBOARD_HOST";
    private static final String DEFAULT_LZY_SNAPSHOT_LOCALHOST = "http://localhost";
    private static URI snapshotURI;

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
        final String lzyWhiteboardHost;
        if (System.getenv().containsKey(LZY_SNAPSHOT_HOST_ENV)) {
            lzyWhiteboardHost = "http://" + System.getenv(LZY_SNAPSHOT_HOST_ENV);
        } else {
            lzyWhiteboardHost = DEFAULT_LZY_SNAPSHOT_LOCALHOST;
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "8999"));
        snapshotURI = URI.create(lzyWhiteboardHost + ":" + port);
        try (ApplicationContext context = ApplicationContext.run(
            PropertySource.of(
                Map.of(
                  "snapshot.uri", snapshotURI.toString()
                )
            )
        )) {
            SnapshotRepository spRepo;
            WhiteboardRepository wbRepo;
            try {
                spRepo = context.getBean(SnapshotRepositoryImpl.class);
                wbRepo = context.getBean(WhiteboardRepositoryImpl.class);
            }
            catch (NoSuchBeanException e){
                LOG.info("LzySnapshot:: Running in inmemory mode");
                spRepo = new InMemRepo();
                wbRepo = (WhiteboardRepository) spRepo;
            }
            SnapshotApi spImpl = new SnapshotApi(spRepo);
            WhiteboardApi wbImpl = new WhiteboardApi(wbRepo, spRepo);
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
