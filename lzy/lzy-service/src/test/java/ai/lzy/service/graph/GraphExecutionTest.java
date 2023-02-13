package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.util.List;

import static ai.lzy.test.IdempotencyUtils.processSequentially;
import static org.junit.Assert.*;

public class GraphExecutionTest extends AbstractGraphExecutionTest {

    @Test
    public void executeSimpleGraph() {
        processSequentially(executeSimpleGraphScenario());
    }

    @Test
    public void executeSequenceOfGraphs() {
        var workflowName = "workflow_1";
        var s3locator =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var createWorkflowResponse = authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setSnapshotStorage(s3locator).build());
        var executionId = createWorkflowResponse.getExecutionId();

        var firstOperation =
            LWF.Operation.newBuilder()
                .setName("operation-1")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
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
                .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/b")
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
                .setCommand("/tmp/lzy_worker_3/sbin/cat /tmp/lzy_worker_3/a")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_3/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", s3locator))
                    .build())
                .setPoolSpecName("s")
                .build(),
            LWF.Operation.newBuilder()
                .setName("operation-3-2")
                .setCommand("/tmp/lzy_worker_3/sbin/cat /tmp/lzy_worker_3/b")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_3/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", s3locator))
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
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(firstGraph)
                .build());

        LWFS.ExecuteGraphResponse secondGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(secondGraph)
                .build());

        LWFS.ExecuteGraphResponse thirdGraphExecution = authorizedWorkflowClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(thirdGraph)
                .build());

        List.of(firstGraphExecution, secondGraphExecution, thirdGraphExecution)
            .forEach(response -> assertFalse(response.getGraphId().isBlank()));
    }

    @Test
    public void failedWithEmptyGraph() {
        processSequentially(emptyGraphScenario());
    }

    @Test
    public void failedWithDuplicatedOutputSlotUris() {
        processSequentially(duplicatedSlotScenario());
    }

    @Test
    public void failedWithCyclicDataflowGraph() {
        processSequentially(cyclicDataflowGraphScenario());
    }

    @Test
    public void failedWithUnknownInputSlotUri() {
        processSequentially(unknownInputSlotUriScenario());
    }

    @Test
    public void failedWithoutSuitableZone() {
        processSequentially(withoutSuitableZoneScenario());
    }

    @Test
    public void failedWithNonSuitableZone() {
        processSequentially(nonSuitableZoneScenario());
    }

    @Test
    public void failedWithAlreadyUsedSlotUri() {
        var workflowName = "workflow_1";
        var storageConfig =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        LWFS.StartWorkflowResponse workflow = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).setSnapshotStorage(storageConfig)
                .build());

        var firstOperation =
            LWF.Operation.newBuilder()
                .setName("prints string to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
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
                .setCommand("/tmp/lzy_worker_2/sbin/cat /tmp/lzy_worker_2/a > /tmp/lzy_worker_2/b " +
                    "&& echo 'hello' > /tmp/lzy_worker_2/a")
                .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/b")
                    .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                    .build())
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_2/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build();

        var secondGraph = LWF.Graph.newBuilder()
            .setName("simple-graph-2")
            .addOperations(secondOperation)
            .build();

        var executionId = workflow.getExecutionId();

        var firstGraphId =
            authorizedWorkflowClient.executeGraph(
                LWFS.ExecuteGraphRequest.newBuilder()
                    .setWorkflowName(workflowName)
                    .setExecutionId(executionId)
                    .setGraph(firstGraph)
                    .build()).getGraphId();
        var errorCode = assertThrows(StatusRuntimeException.class,
            () -> {
                //noinspection ResultOfMethodCallIgnored
                authorizedWorkflowClient.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(executionId)
                        .setGraph(secondGraph)
                        .build());
            }).getStatus().getCode();

        assertFalse(firstGraphId.isBlank());
        assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode);
    }

    @Test
    public void failedWithUnknownExecutionId() {
        var workflowName = "workflow_1";
        var storageConfig =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                    LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        LWFS.StartWorkflowResponse workflow = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).setSnapshotStorage(storageConfig)
                .build());

        var operations = List.of(
            LWF.Operation.newBuilder()
                .setName("first task prints string 'i-am-hacker' to variable")
                .setCommand("echo 'i-am-a-hacker' > /tmp/lzy_worker_1/a")
                .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                    .setPath("/tmp/lzy_worker_1/a")
                    .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                    .build())
                .setPoolSpecName("s")
                .build());
        var graph = LWF.Graph.newBuilder()
            .setName("simple-graph")
            .setZone("ru-central1-a")
            .addAllOperations(operations)
            .build();

        var executionId = workflow.getExecutionId();
        var invalidExecutionId = executionId + "_invalid_prefix";

        var thrown = assertThrows(StatusRuntimeException.class, () -> {
            //noinspection ResultOfMethodCallIgnored
            authorizedWorkflowClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(invalidExecutionId)
                .setGraph(graph)
                .build());
        });

        assertEquals(Status.NOT_FOUND.getCode(), thrown.getStatus().getCode());
    }
}
