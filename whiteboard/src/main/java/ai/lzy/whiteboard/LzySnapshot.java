package ai.lzy.whiteboard;

import ai.lzy.whiteboard.api.SnapshotApi;
import ai.lzy.whiteboard.api.WhiteboardApi;
import ai.lzy.whiteboard.config.ServerConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LzySnapshot {

    private static final Logger LOG = LogManager.getLogger(LzySnapshot.class);

    private static final String LZY_SNAPSHOT_HOST_ENV = "LZY_SNAPSHOT_HOST";

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            ServerConfig config = context.getBean(ServerConfig.class);
            int port = config.getPort();
            final String lzyWhiteboardHost;
            if (System.getenv().containsKey(LZY_SNAPSHOT_HOST_ENV)) {
                lzyWhiteboardHost = System.getenv(LZY_SNAPSHOT_HOST_ENV);
            } else {
                lzyWhiteboardHost = "localhost";
            }
            LOG.info("Starting at: " + lzyWhiteboardHost + ":" + port);

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
