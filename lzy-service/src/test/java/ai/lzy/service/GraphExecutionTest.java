package ai.lzy.service;

import ai.lzy.v1.graph.GraphExecutorGrpc;
import io.grpc.ManagedChannel;
import org.junit.Test;

public class GraphExecutionTest extends BaseTest {

    private ManagedChannel graphExecutorChannel;
    private GraphExecutorGrpc.GraphExecutorBlockingStub graphExecutorClient;

//    @Override
//    @Before
//    public void setUp() throws IOException, InterruptedException {
//        super.setUp();
//
//        graphExecutorChannel = ChannelBuilder.forAddress().build();
//        graphExecutorClient = GraphExecutorGrpc.newBlockingStub(graphExecutorChannel);
//    }

    @Test
    public void executeSimpleGraph() {

    }
}
