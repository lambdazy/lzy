package ai.lzy.portal.mocks;

import ai.lzy.channelmanager.grpc.ChannelManagerMock;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.whiteboard.WhiteboardPrivateApiMock;
import io.grpc.Server;

import java.io.IOException;

public class MocksServer {
    private final Server server;

    private final SchedulerPrivateApiMock schedulerMock;
    private final ChannelManagerMock channelManagerMock;

    public MocksServer(int port) {
        this.schedulerMock = new SchedulerPrivateApiMock();
        this.channelManagerMock = new ChannelManagerMock();

        this.server = GrpcUtils.newGrpcServer("localhost", port, GrpcUtils.NO_AUTH)
            .addService(schedulerMock)
            .addService(channelManagerMock.publicService)
            .addService(channelManagerMock.privateService)
            .addService(new AllocatorPrivateAPIMock())
            .addService(new WhiteboardPrivateApiMock())
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() throws InterruptedException {
        schedulerMock.stop();
        channelManagerMock.stop();
        server.shutdown();
        server.awaitTermination();
    }

    public SchedulerPrivateApiMock getSchedulerMock() {
        return schedulerMock;
    }

    public ChannelManagerMock getChannelManagerMock() {
        return channelManagerMock;
    }
}
