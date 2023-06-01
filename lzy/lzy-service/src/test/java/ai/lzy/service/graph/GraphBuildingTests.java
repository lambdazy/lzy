package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.lzy.service.Graphs.buildSlotUri;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class GraphBuildingTests extends AbstractGraphExecutionTest {
    @Test
    public void executeSimpleGraph() {
        var workflow = startWorkflow("workflow_1");

        var graphId = authLzyGrpcClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(simpleGraph())
                .build())
            .getGraphId();

        assertFalse(graphId.isBlank());
    }

    @Test
    public void executeGraphWithoutOutputSlots() {
        var workflow = startWorkflow("workflow_1");

        var graphId = authLzyGrpcClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(withMissingOutputSlot())
                .build())
            .getGraphId();

        assertFalse(graphId.isBlank());
    }

    @Test
    public void repeatedOpsCollapseToSingle() {
        var workflow = startWorkflow("workflow_1");
        var countOfTasks = new AtomicInteger(0);

        graphExecutor().setOnExecute(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphId = authLzyGrpcClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphWithRepeatedOps())
                .build())
            .getGraphId();

        assertSame(2, countOfTasks.get());
    }

    @Test
    public void executeGraphWithSingleProducerMultipleConsumers() {
        var workflowName = "workflow_1";
        var s3locator = authLzyGrpcClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var createWorkflowResponse = authLzyGrpcClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setSnapshotStorage(s3locator).build());
        var executionId = createWorkflowResponse.getExecutionId();

        var graphs = producerAndConsumersGraphs();

        var graphId1 = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        var graphId2 = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        var graphId3 = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graphs.get(2))
                .build()).getGraphId();

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertFalse(graphId3.isBlank());
    }

    @Ignore
    @Test
    public void executeSequenceOfGraphs() {
        var workflowName = "workflow_1";
        var s3locator = authLzyGrpcClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var createWorkflowResponse = authLzyGrpcClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setSnapshotStorage(s3locator).build());
        var executionId = createWorkflowResponse.getExecutionId();

        var firstOperation =
            LWF.Operation.newBuilder()
                .setName("operation-1")
                .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
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
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", s3locator))
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
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("operation-3-2")
                .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", s3locator))
                    .build())
                .setPoolSpecName("s")
                .build()
        );

        var thirdGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-3")
            .addAllOperations(thirdOperations)
            .build();

        var firstGraphId = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(firstGraph)
                .build()).getGraphId();

        var secondGraphId = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(secondGraph)
                .build()).getGraphId();

        var thirdGraphId = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(thirdGraph)
                .build()).getGraphId();

        List.of(firstGraphId, secondGraphId, thirdGraphId).forEach(graphId -> assertFalse(graphId.isBlank()));
    }
}
