package ai.lzy.test.scenarios;

import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.GraphExecutorContext;
import ai.lzy.test.impl.v2.WorkflowContext;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static org.junit.Assert.*;

public class CachedGraphExecutionTest {
    @Rule
    public final ApplicationContextRule ctx = new ApplicationContextRule();

    @Rule
    public final ContextRule<WorkflowContext> workflow = new ContextRule<>(ctx, WorkflowContext.class);
    @Rule
    public final ContextRule<GraphExecutorContext> graph = new ContextRule<>(ctx, GraphExecutorContext.class);

    LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub workflowStub;
    LongRunningServiceGrpc.LongRunningServiceBlockingStub operationsStub;
    LMST.StorageConfig storageConfig;

    @Before
    public void setUp() {
        workflowStub = workflow.context().stub();
        operationsStub = workflow.context().opsStub();
        storageConfig = workflowStub.getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder()
            .build()).getStorage();
    }

    @Test
    public void repeatedExecsUseCache() throws InvalidProtocolBufferException {
        var countOfTasks = new AtomicInteger(0);

        onExecuteGraph(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphs = sequenceOfGraphs(storageConfig);

        var exec1 = startWorkflow("workflow_1");
        var graphId1 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(exec1.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        awaitGraph(exec1.getExecutionId(), graphId1);
        finishWorkflow("workflow_1", exec1.getExecutionId());

        var exec2 = startWorkflow("workflow_1");
        var graphId2 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(exec2.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        awaitGraph(exec2.getExecutionId(), graphId2);
        finishWorkflow("workflow_1", exec2.getExecutionId());

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertEquals(2, countOfTasks.get());
    }

    @Test
    public void executeFullyCachedGraph() throws InvalidProtocolBufferException {
        var workflow = startWorkflow("workflow_1");
        var graphs = sequenceOfGraphs(storageConfig);
        var countOfGraphs = new AtomicInteger(0);

        onExecuteGraph(request -> countOfGraphs.incrementAndGet());

        var graphId1 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();
        awaitGraph(workflow.getExecutionId(), graphId1);

        var graphId2 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();
        awaitGraph(workflow.getExecutionId(), graphId2);

        var before = countOfGraphs.get();

        var graphId3 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(2))
                .build()).getGraphId();

        var after = countOfGraphs.get();

        finishWorkflow("workflow_1", workflow.getExecutionId());

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertTrue(graphId3.isBlank());
        assertSame(before, after);
    }

    @Test
    public void repeatedGraphsUseCache() throws InvalidProtocolBufferException {
        var workflow = startWorkflow("workflow_1");
        var countOfTasks = new AtomicInteger(0);

        onExecuteGraph(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphs = sequenceOfGraphs(storageConfig);

        var graphId1 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        awaitGraph(workflow.getExecutionId(), graphId1);

        var graphId2 = workflowStub.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder().setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        awaitGraph(workflow.getExecutionId(), graphId2);
        finishWorkflow("workflow_1", workflow.getExecutionId());

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertEquals(2, countOfTasks.get());
    }

    void onExecuteGraph(Consumer<GraphExecutorApi.GraphExecuteRequest> action) {
        graph.context().onExecuteGraph(action);
    }

    LWFS.StartWorkflowResponse startWorkflow(String name) {
        return workflowStub.startWorkflow(LWFS.StartWorkflowRequest
            .newBuilder()
            .setWorkflowName(name)
            .setSnapshotStorage(storageConfig)
            .build()
        );
    }

    LWFS.FinishWorkflowResponse finishWorkflow(String name, String activeExecutionId)
        throws InvalidProtocolBufferException
    {
        var finishOp = workflowStub.finishWorkflow(
            LWFS.FinishWorkflowRequest.newBuilder()
                .setWorkflowName(name)
                .setExecutionId(activeExecutionId)
                .build());
        finishOp = awaitOperationDone(operationsStub, finishOp.getId(), Duration.ofSeconds(10));
        return finishOp.getResponse().unpack(LWFS.FinishWorkflowResponse.class);
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
        var firstGraph = LWF.Graph.newBuilder().setName("single-out").addOperations(firstOp).build();

        var secondOp = LWF.Operation.newBuilder()
            .setName("operation-2")
            .setCommand("$LZY_MOUNT/sbin/cat $LZY_MOUNT/a > $LZY_MOUNT/b")
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
        var secondGraph = LWF.Graph.newBuilder().setName("in-out").addAllOperations(
            List.of(firstOp, secondOp)).build();

        var sameAsSecond = LWF.Graph.newBuilder().setName("fully-cached").addAllOperations(List.of(
            firstOp, secondOp)).build();

        return List.of(firstGraph, secondGraph, sameAsSecond);
    }

    static String buildSlotUri(String key, LMST.StorageConfig storageConfig) {
        return storageConfig.getUri() + "/" + key;
    }

    void awaitGraph(String executionId, String graphId) {
        LWFS.GraphStatusResponse status;

        do {
            status = workflowStub.graphStatus(LWFS.GraphStatusRequest.newBuilder()
                .setExecutionId(executionId)
                .setGraphId(graphId)
                .build());
        } while (!status.hasCompleted() && !status.hasFailed());
    }
}
