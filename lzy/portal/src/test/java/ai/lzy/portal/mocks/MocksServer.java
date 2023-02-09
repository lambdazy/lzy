package ai.lzy.portal.mocks;

import ai.lzy.util.grpc.GrpcUtils;
import io.grpc.Server;

import java.io.IOException;

public class MocksServer {
    private final Server server;

    public MocksServer(int port) {

        this.server = GrpcUtils.newGrpcServer("localhost", port, GrpcUtils.NO_AUTH)
            .addService(new AllocatorPrivateAPIMock())
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
    }

}
