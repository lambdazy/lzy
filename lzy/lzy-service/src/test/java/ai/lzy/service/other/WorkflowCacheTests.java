package ai.lzy.service.other;

import ai.lzy.service.ContextAwareTests;
import ai.lzy.service.Graphs;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWFS;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WorkflowCacheTests extends ContextAwareTests {
    private LMST.StorageConfig createStorage() {
        return authLzyGrpcClient.getOrCreateDefaultStorage(
            LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
    }

    private String startWorkflow(String workflowName, LMST.StorageConfig storage, String idempotencyKey) {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(storage)
            .build();
        return withIdempotencyKey(authLzyGrpcClient, idempotencyKey).startWorkflow(request).getExecutionId();
    }

    private void finishWorkflow(String workflowName, String executionId, String idempotencyKey) {
        var request = LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setReason("no-matter")
            .build();
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(authLzyGrpcClient, idempotencyKey).finishWorkflow(request);
    }

    @Test
    public void workflowsNotShareCache() {
        var storage = createStorage();
        var graphs = Graphs.sequenceOfGraphs(storage);
        var countOfTasks = new AtomicInteger(0);

        graphExecutor().setOnExecute(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var execId1 = startWorkflow("workflow_1", storage, "start_wf_1");
        var graphId1 = withIdempotencyKey(authLzyGrpcClient, "execute_graph_1").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(execId1)
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        finishWorkflow("workflow_1", execId1, "finish_wf_1");

        var execId2 = startWorkflow("workflow_2", storage, "start_wf_2");
        var graphId2 = withIdempotencyKey(authLzyGrpcClient, "execute_graph_2").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_2")
                .setExecutionId(execId2)
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        finishWorkflow("workflow_2", execId2, "finish_wf_2");

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertEquals(3, countOfTasks.get());
    }
}
