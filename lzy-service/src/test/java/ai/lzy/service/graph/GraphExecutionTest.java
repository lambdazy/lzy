package ai.lzy.service.graph;

import ai.lzy.graph.test.GraphExecutorMock;
import ai.lzy.service.BaseTest;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
    public void executeSimpleGraph() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second task reads string 'i-am-hacker' from variable and prints it to another one")
                .setCommand("/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/a > /tmp/lzy_servant_2/b")
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

    @Test
    public void executeSequenceOfGraphs() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var firstOperation =
            LWF.Operation.newBuilder()
                .setName("operation-1")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build();
        var firstGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-1")
            .addOperations(firstOperation)
            .build();

        var secondOperation =
            LWF.Operation.newBuilder()
                .setName("operation-2")
                .setCommand("/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/a > /tmp/lzy_servant_2/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/b")
                    .setStorageUri("snapshot_b_1")
                    .build())
                .setPoolSpecName("s")
                .build();

        var secondGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-2")
            .addOperations(secondOperation)
            .build();

        var thirdOperations = List.of(
            LWF.Operation.newBuilder()
                .setName("operation-3-1")
                .setCommand("/tmp/lzy_servant_3/sbin/cat /tmp/lzy_servant_3/a")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_3/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("operation-3-2")
                .setCommand("/tmp/lzy_servant_3/sbin/cat /tmp/lzy_servant_3/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_3/b")
                    .setStorageUri("snapshot_b_1")
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        var thirdGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-3")
            .addAllOperations(thirdOperations)
            .build();

        LWFS.ExecuteGraphResponse firstGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(firstGraph)
                .build());

        LWFS.ExecuteGraphResponse secondGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(secondGraph)
                .build());

        LWFS.ExecuteGraphResponse thirdGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(thirdGraph)
                .build());

        List.of(firstGraphExecution, secondGraphExecution, thirdGraphExecution)
            .forEach(response -> Assert.assertFalse(response.getGraphId().isBlank()));
    }

    @Test
    public void failedWithUnknownExecutionId() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();
        var invalidExecutionId = executionId + "_invalid_prefix";

        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build());
        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .setZone("ru-central1-a")
            .addAllOperations(operations)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(invalidExecutionId)
                .setGraph(graph)
                .build()));

        Assert.assertEquals(Status.NOT_FOUND.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithEmptyGraph() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .build();

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()));

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithDuplicatedOutputSlotUris() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var operation =
            LWF.Operation.newBuilder()
                .setName("prints strings to variables")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a && echo 'hello' > /tmp/lzy_servant_1/b")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/b")
                    .setStorageUri("snapshot_a_1"))
                .setPoolSpecName("s")
                .build();
        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .addOperations(operation)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()));

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithAlreadyUsedSlotUri() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var firstOperation =
            LWF.Operation.newBuilder()
                .setName("prints string to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build();
        var firstGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-1")
            .addOperations(firstOperation)
            .build();

        var secondOperation =
            LWF.Operation.newBuilder()
                .setName("prints strings to variables")
                .setCommand("/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/a > /tmp/lzy_servant_2/b " +
                    "&& echo 'hello' > /tmp/lzy_servant_2/a")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/b")
                    .setStorageUri("snapshot_b_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build();

        var secondGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-2")
            .addOperations(secondOperation)
            .build();

        LWFS.ExecuteGraphResponse firstGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(firstGraph)
                .build());

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(
                LWFS.ExecuteGraphRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setGraph(secondGraph)
                    .build()));

        boolean graphIdIsBlank = firstGraphExecution.getGraphId().isBlank();
        Assert.assertFalse(graphIdIsBlank);

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithCyclicDataflowGraph() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var operationsWithCycleDependency = List.of(
            LWF.Operation.newBuilder()
                .setName("first operation")
                .setCommand("echo '42' > /tmp/lzy_servant_1/a && " +
                    "/tmp/lzy_servant_1/sbin/cat /tmp/lzy_servant_1/c > /tmp/lzy_servant_1/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/c")
                    .setStorageUri("snapshot_c_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/b")
                    .setStorageUri("snapshot_b_1")
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("second operation")
                .setCommand("/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/a > /tmp/lzy_servant_2/d &&" +
                    " /tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/d > /tmp/lzy_servant_2/c")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/d")
                    .setStorageUri("snapshot_d_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/c")
                    .setStorageUri("snapshot_c_1")
                    .build())
                .setPoolSpecName("s")
                .build());

        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .addAllOperations(operationsWithCycleDependency)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(
                LWFS.ExecuteGraphRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setGraph(graph)
                    .build()));

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithUnknownInputSlotUri() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var firstOperation =
            LWF.Operation.newBuilder()
                .setName("prints string to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("s")
                .build();
        var firstGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-1")
            .addOperations(firstOperation)
            .build();

        var unknownStorageUri = "snapshot_c_1";

        var secondOperation =
            LWF.Operation.newBuilder()
                .setName("prints strings to variables")
                .setCommand("/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/c > /tmp/lzy_servant_2/b && " +
                    "/tmp/lzy_servant_2/sbin/cat /tmp/lzy_servant_2/a > /tmp/lzy_servant_2/d")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/c")
                    .setStorageUri(unknownStorageUri)
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/b")
                    .setStorageUri("snapshot_b_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_2/d")
                    .setStorageUri("snapshot_d_1")
                    .build())
                .setPoolSpecName("s")
                .build();

        var secondGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-2")
            .addOperations(secondOperation)
            .build();

        LWFS.ExecuteGraphResponse firstGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(firstGraph)
                .build());

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(
                LWFS.ExecuteGraphRequest.newBuilder()
                    .setExecutionId(executionId)
                    .setGraph(secondGraph)
                    .build()));

        boolean graphIdIsBlank = firstGraphExecution.getGraphId().isBlank();
        Assert.assertFalse(graphIdIsBlank);

        Assert.assertEquals(Status.NOT_FOUND.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithoutSuitableZone() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var operation =
            LWF.Operation.newBuilder()
                .setName("prints string to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .setPoolSpecName("m")
                .build();
        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .addOperations(operation)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()));

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void failedWithNonSuitableZone() {
        var workflowName = "workflow_1";
        var executionId = authorizedWorkflowClient.createWorkflow(LWFS.CreateWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).build()).getExecutionId();

        var operation =
            LWF.Operation.newBuilder()
                .setName("prints strings to variables")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_servant_1/a && echo 'hello' > /tmp/lzy_servant_1/b")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/a")
                    .setStorageUri("snapshot_a_1")
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_servant_1/b")
                    .setStorageUri("snapshot_b_1"))
                .setPoolSpecName("l")
                .build();

        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .setZone("ru-central1-a")
            .addOperations(operation)
            .build();

        //noinspection ResultOfMethodCallIgnored
        var thrown = Assert.assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()));

        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }
}
