package ai.lzy.whiteboard;

import ai.lzy.whiteboard.api.SnapshotApi;
import ai.lzy.whiteboard.api.WhiteboardApi;
import ai.lzy.whiteboard.config.ServiceConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class LzySnapshot {

    private static final Logger LOG = LogManager.getLogger(LzySnapshot.class);

    private static final String LZY_SNAPSHOT_HOST_ENV = "LZY_SNAPSHOT_HOST";

    public static void main(String[] args) throws IOException, InterruptedException {
        try (ApplicationContext context = ApplicationContext.run()) {
            ServiceConfig config = context.getBean(ServiceConfig.class);
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
