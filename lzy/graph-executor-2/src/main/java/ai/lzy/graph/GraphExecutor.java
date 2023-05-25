package ai.lzy.graph;

import java.io.IOException;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class GraphExecutor {
    private static final Logger LOG = LogManager.getLogger(GraphExecutor.class);

    public static final String APP = "LzyGraphExecutor2";

    private final ServiceConfig config;
    private final ManagedChannel iamChannel;
    private final GraphExecutorApi graphExecutorApi;
    private Server server;

    @Inject
    public GraphExecutor(ServiceConfig config,
                         @Named("GraphExecutorIamGrpcChannel") ManagedChannel iamChannel,
                         GraphExecutorApi graphExecutorApi)
    {
        this.config = config;
        this.iamChannel = iamChannel;
        this.graphExecutorApi = graphExecutorApi;
    }

    public void start() throws InterruptedException, IOException {
        LOG.info("Starting GraphExecutor2 service...");

        final var internalUserOnly = new AllowInternalUserOnlyInterceptor(APP, iamChannel);

        server =
            newGrpcServer("0.0.0.0", config.getPort(),
                new AuthServerInterceptor(new AuthenticateServiceGrpcClient(APP, iamChannel)))
                .addService(ServerInterceptors.intercept(graphExecutorApi, internalUserOnly))
                .build();

        server.start();

        LOG.info("Starting GraphExecutor2 service done");
    }

    public void close() {
        LOG.info("Shutdown graph executor 2...");
        if (server != null) {
            server.shutdown();
        }
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) {
        try (var context = ApplicationContext.run()) {
            final GraphExecutor graphExecutor = context.getBean(GraphExecutor.class);
            final Thread thread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Stopping GraphExecutor2 service...");
                graphExecutor.close();
                while (true) {
                    try {
                        thread.join();
                        break;
                    } catch (InterruptedException e) {
                        LOG.debug(e);
                    }
                }
                LOG.info("Stopping GraphExecutor2 service done");
            }));
            try {
                graphExecutor.start();
                graphExecutor.awaitTermination();
            } catch (InterruptedException | IOException e) {
                LOG.error("Error while starting GraphExecutor2", e);
            }
        }
    }

}
