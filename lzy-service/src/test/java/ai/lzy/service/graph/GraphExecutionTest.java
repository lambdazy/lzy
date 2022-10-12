package ai.lzy.service.graph;

import ai.lzy.graph.test.GraphExecutorMock;
import ai.lzy.service.BaseTest;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GraphExecutionTest extends BaseTest {

    private Server graphExecutorServer;

    @Override
    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();

        var graphExecutorAddress = HostAndPort.fromString(config.getGraphExecutorAddress());

        graphExecutorMock = new GraphExecutorMock();
        graphExecutorServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(graphExecutorAddress.getHost(), graphExecutorAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(graphExecutorMock, authInterceptor))
            .build();
        graphExecutorServer.start();
    }

    @Override
    @After
    public void tearDown() throws java.sql.SQLException, InterruptedException {
        super.tearDown();
        graphExecutorServer.shutdown();
    }

    @Test
    public void executeSimpleGraphViaLzyService() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
                .setWorkflowName(workflowName).build()).getExecutionId();

        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("a = 'i-am-a-hacker'\nprint('hello')")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("b = a\nprint(b)")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/b")
                    .setStorageUri("snapshot_b_1")
                    .build())
                .setPoolSpecName("s")
                .build()
        );
        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .setZone("ru-central1-a")
            .addAllOperations(operations)
            .build();

        LWFS.ExecuteGraphResponse executedGraph = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(graph)
                .build());

        boolean graphIdIsBlank = executedGraph.getGraphId().isBlank();
        Assert.assertFalse(graphIdIsBlank);
    }
}
