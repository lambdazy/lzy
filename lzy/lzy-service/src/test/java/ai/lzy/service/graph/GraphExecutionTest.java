package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class GraphExecutionTest extends AbstractGraphExecutionTest {
    @Test
    public void executeSimpleGraph() {
        var workflow = startWorkflow("workflow_1");

        var graphId = authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
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

        var graphId = authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
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

        onExecuteGraph(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphId = authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphWithRepeatedOps())
                .build())
            .getGraphId();

        assertSame(2, countOfTasks.get());
    }

    @Test
    public void workflowsNotShareCache() throws InvalidProtocolBufferException {
        var graphs = sequenceOfGraphs(storageConfig);
        var countOfTasks = new AtomicInteger(0);

        onExecuteGraph(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var workflow1 = startWorkflow("workflow_1");
        var graphId1 = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(workflow1.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        finishWorkflow("workflow_1", workflow1.getExecutionId());

        var workflow2 = startWorkflow("workflow_2");
        var graphId2 = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_2")
                .setExecutionId(workflow2.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        finishWorkflow("workflow_2", workflow2.getExecutionId());

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertEquals(3, countOfTasks.get());
    }

    @Test
    public void executeGraphWithSingleProducerMultipleConsumers() {
        var workflowName = "workflow_1";
        var s3locator = authorizedWorkflowClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var createWorkflowResponse = authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setSnapshotStorage(s3locator).build());
        var executionId = createWorkflowResponse.getExecutionId();

        var graphs = producerAndConsumersGraphs();

        var graphId1 = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        var graphId2 = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        var graphId3 = authorizedWorkflowClient.executeGraph(
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
        var s3locator = authorizedWorkflowClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var createWorkflowResponse = authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
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

        var firstGraphId = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(firstGraph)
                .build()).getGraphId();

        var secondGraphId = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(secondGraph)
                .build()).getGraphId();

        var thirdGraphId = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(thirdGraph)
                .build()).getGraphId();

        List.of(firstGraphId, secondGraphId, thirdGraphId).forEach(graphId -> assertFalse(graphId.isBlank()));
    }

    @Test
    public void failedWithUnknownExecutionId() {
        var workflow = startWorkflow("workflow_1");

        var thrown = assertThrows(StatusRuntimeException.class, () -> {
            //noinspection ResultOfMethodCallIgnored
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId() + "_invalid_prefix")
                .setGraph(simpleGraph())
                .build());
        });

        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Ignore
    @Test
    public void failedWithAlreadyUsedSlotUri() {
        var workflow = startWorkflow("workflow_1");

        var firstGraph = LWF.Graph.newBuilder()
            .setName("graph-1")
            .addOperations(LWF.Operation.newBuilder()
                .setName("foo")
                .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build())
            .build();

        var secondGraph = LWF.Graph.newBuilder()
            .setName("graph-2")
            .addOperations(LWF.Operation.newBuilder()
                .setName("bar")
                .setCommand("echo 'hello' > $LZY_MOUNT/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build())
            .build();

        var graphId = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(firstGraph)
                .build()).getGraphId();
        var errorCode = assertThrows(StatusRuntimeException.class, () -> {
            //noinspection ResultOfMethodCallIgnored
            authorizedWorkflowClient.executeGraph(
                LWFS.ExecuteGraphRequest.newBuilder()
                    .setWorkflowName("workflow_1")
                    .setExecutionId(workflow.getExecutionId())
                    .setGraph(secondGraph)
                    .build());
        }).getStatus().getCode();

        assertFalse(graphId.isBlank());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode);
    }
}
