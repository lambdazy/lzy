package ai.lzy.test;

import ai.lzy.test.context.LzyContextTests;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.*;

public class CachedGraphExecutionTest extends LzyContextTests {
    LMST.StorageConfig storageConfig;

    @Before
    public void before() {
        storageConfig = lzyGrpcClient.getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder()
            .build()).getStorage();
    }

    @Test
    public void repeatedExecsUseCache() {
        var countOfTasks = new AtomicInteger(0);

        onExecuteGraph(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphs = sequenceOfGraphs(storageConfig);

        var wfName = "workflow_1";
        var exec1 = startWorkflow(wfName, "start_wf_1");
        var graphId1 = withIdempotencyKey(lzyGrpcClient, "exec_graph_1").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(exec1.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        awaitGraph(wfName, exec1.getExecutionId(), graphId1);
        finishWorkflow(wfName, exec1.getExecutionId(), "finish_wf_1");

        var exec2 = startWorkflow(wfName, "start_wf_2");
        var graphId2 = withIdempotencyKey(lzyGrpcClient, "exec_graph_2").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(exec2.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        awaitGraph(wfName, exec2.getExecutionId(), graphId2);
        finishWorkflow(wfName, exec2.getExecutionId(), "finish_wf_2");

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertEquals(2, countOfTasks.get());
    }

    @Test
    public void executeFullyCachedGraph() {
        var wfName = "workflow_1";
        var workflow = startWorkflow(wfName, "start_wf");
        var graphs = sequenceOfGraphs(storageConfig);
        var countOfGraphs = new AtomicInteger(0);

        onExecuteGraph(request -> countOfGraphs.incrementAndGet());

        var graphId1 = withIdempotencyKey(lzyGrpcClient, "exec_graph_1").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();
        awaitGraph(wfName, workflow.getExecutionId(), graphId1);

        var graphId2 = withIdempotencyKey(lzyGrpcClient, "exec_graph_2").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();
        awaitGraph(wfName, workflow.getExecutionId(), graphId2);

        var before = countOfGraphs.get();

        var graphId3 = withIdempotencyKey(lzyGrpcClient, "exec_graph_3").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(2))
                .build()).getGraphId();

        var after = countOfGraphs.get();

        finishWorkflow(wfName, workflow.getExecutionId(), "finish_wf");

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertTrue(graphId3.isBlank());
        assertSame(before, after);
    }

    @Test
    public void repeatedGraphsUseCache() {
        var wfName = "workflow_1";
        var workflow = startWorkflow(wfName, "start_wf");
        var countOfTasks = new AtomicInteger(0);

        onExecuteGraph(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphs = sequenceOfGraphs(storageConfig);

        var graphId1 = withIdempotencyKey(lzyGrpcClient, "exec_graph_1").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        awaitGraph(wfName, workflow.getExecutionId(), graphId1);

        var graphId2 = withIdempotencyKey(lzyGrpcClient, "exec_graph_2").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName(wfName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        awaitGraph(wfName, workflow.getExecutionId(), graphId2);
        finishWorkflow(wfName, workflow.getExecutionId(), "finish_wf");

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertEquals(2, countOfTasks.get());
    }

    void onExecuteGraph(Consumer<GraphExecutorApi.GraphExecuteRequest> action) {
        graphExecutorDecorator().setOnExecute(action);
    }

    LWFS.StartWorkflowResponse startWorkflow(String name, String idempotencyKey) {
        return withIdempotencyKey(lzyGrpcClient, idempotencyKey).startWorkflow(LWFS.StartWorkflowRequest
            .newBuilder()
            .setWorkflowName(name)
            .setSnapshotStorage(storageConfig)
            .build()
        );
    }

    void finishWorkflow(String name, String activeExecutionId, String idempotencyKey) {
        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyGrpcClient, idempotencyKey).finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(name)
                .setExecutionId(activeExecutionId)
                .setReason("no-matter")
                .build());
    }

    /*  Graphs:
        single_out:   nothing --> 1
        in_out:       1 --> 2
        fully_cached: 1 --> 2
    */
    List<LWF.Graph> sequenceOfGraphs(LMST.StorageConfig storageConfig) {
        var firstOp = LWF.Operation.newBuilder()
            .setName("operation-1")
            .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();
        var firstGraph = LWF.Graph.newBuilder()
            .setName("single-out")
            .addOperations(firstOp)
            .addDataDescriptions(
                LWF.DataDescription.newBuilder().setStorageUri(buildSlotUri("snapshot_a_1", storageConfig)).build()
            ).build();

        var secondOp = LWF.Operation.newBuilder()
            .setName("operation-2")
            .setCommand("cat $LZY_MOUNT/a > $LZY_MOUNT/b")
            .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/a")
                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
                .build())
            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
                .setPath("/b")
                .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
                .build())
            .setPoolSpecName("s")
            .build();

        var dataDescriptions = List.of(
            LWF.DataDescription.newBuilder().setStorageUri(buildSlotUri("snapshot_a_1", storageConfig)).build(),
            LWF.DataDescription.newBuilder().setStorageUri(buildSlotUri("snapshot_b_1", storageConfig)).build()
        );

        var secondGraph = LWF.Graph.newBuilder().setName("in-out").addAllOperations(
            List.of(firstOp, secondOp)).addAllDataDescriptions(dataDescriptions).build();

        var sameAsSecond = LWF.Graph.newBuilder().setName("fully-cached").addAllOperations(List.of(
            firstOp, secondOp)).addAllDataDescriptions(dataDescriptions).build();

        return List.of(firstGraph, secondGraph, sameAsSecond);
    }


    //    /*  Graphs:
//        single_out:   nothing --> 1
//        in_out:       1 --> 2
//        fully_cached: 1 --> 2
//    */
//    List<LWF.Graph> sequenceOfGraphs(LMST.StorageConfig storageConfig) {
//        var firstOp = LWF.Operation.newBuilder()
//            .setName("operation-1")
//            .setCommand("echo 'i-am-a-hacker' > $LZY_MOUNT/a")
//            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
//                .setPath("/a")
//                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
//                .build())
//            .setPoolSpecName("s")
//            .build();
//        var firstGraph = LWF.Graph.newBuilder().setName("single-out").addOperations(firstOp).build();
//
//        var secondOp = LWF.Operation.newBuilder()
//            .setName("operation-2")
//            .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
//            .addInputSlots(LWF.Operation.SlotDescription.newBuilder()
//                .setPath("/a")
//                .setStorageUri(buildSlotUri("snapshot_a_1", storageConfig))
//                .build())
//            .addOutputSlots(LWF.Operation.SlotDescription.newBuilder()
//                .setPath("/b")
//                .setStorageUri(buildSlotUri("snapshot_b_1", storageConfig))
//                .build())
//            .setPoolSpecName("s")
//            .build();
//        var secondGraph = LWF.Graph.newBuilder().setName("in-out").addAllOperations(
//            List.of(firstOp, secondOp)).build();
//
//        var sameAsSecond = LWF.Graph.newBuilder().setName("fully-cached").addAllOperations(List.of(
//            firstOp, secondOp)).build();
//
//        return List.of(firstGraph, secondGraph, sameAsSecond);
//    }
//
    static String buildSlotUri(String key, LMST.StorageConfig storageConfig) {
        return storageConfig.getUri() + "/" + key;
    }

    void awaitGraph(String workflowName, String executionId, String graphId) {
        LWFS.GraphStatusResponse status;

        do {
            status = lzyGrpcClient.graphStatus(LWFS.GraphStatusRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraphId(graphId)
                .build());
        } while (!status.hasCompleted() && !status.hasFailed());
    }
}
