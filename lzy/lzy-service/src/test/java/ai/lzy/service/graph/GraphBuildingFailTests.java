package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static ai.lzy.service.Graphs.buildSlotUri;
import static org.junit.Assert.*;

public class GraphBuildingFailTests extends AbstractGraphExecutionTest {
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

        var graphId = authLzyGrpcClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(firstGraph)
                .build()).getGraphId();
        var errorCode = assertThrows(StatusRuntimeException.class, () -> {
            //noinspection ResultOfMethodCallIgnored
            authLzyGrpcClient.executeGraph(
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
