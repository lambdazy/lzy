package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micronaut.context.ApplicationContext;
import java.io.IOException;

/**
 * InProcessServer that manages startup/shutdown of a service within the same process as the client is running. Used for
 * unit testing purposes.
 *
 * @author be
 */
public class InProcessServer<T extends io.grpc.BindableService> {

    private Server server;

    private T instance;
    private ApplicationContext ctx;

    public InProcessServer(T instance) {
        this.instance = instance;
    }

    public void start() throws IOException {
        server = InProcessServerBuilder
            .forName("test")
            .directExecutor()
            .addService(instance)
            .build()
            .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                InProcessServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
