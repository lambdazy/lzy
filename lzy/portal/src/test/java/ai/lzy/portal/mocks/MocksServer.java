package ai.lzy.portal.mocks;

import ai.lzy.util.grpc.GrpcUtils;
import io.grpc.Server;

import java.io.IOException;

public class MocksServer {
    private final Server server;

    private final SchedulerPrivateApiMock schedulerMock;

    public MocksServer(int port) {
        this.schedulerMock = new SchedulerPrivateApiMock();

        this.server = GrpcUtils.newGrpcServer("localhost", port, GrpcUtils.NO_AUTH)
            .addService(schedulerMock)
            .addService(new AllocatorPrivateAPIMock())
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() throws InterruptedException {
        schedulerMock.stop();
        server.shutdown();
        server.awaitTermination();
    }

    public SchedulerPrivateApiMock getSchedulerMock() {
        return schedulerMock;
    }

}
