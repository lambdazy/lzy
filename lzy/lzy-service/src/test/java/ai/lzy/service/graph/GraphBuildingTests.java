package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWFS;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class GraphBuildingTests extends AbstractGraphExecutionTest {
    @Test
    public void executeSimpleGraph() {
        var workflow = startWorkflow("workflow_1", "start_wf");

        var graphId = withIdempotencyKey(authLzyGrpcClient, "execute_graph")
            .executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(simpleGraph())
                .build())
            .getGraphId();

        finishWorkflow("workflow_1", workflow.getExecutionId(), "finish_wf");

        assertFalse(graphId.isBlank());
    }

    @Test
    public void executeGraphWithoutOutputSlots() {
        var workflow = startWorkflow("workflow_1", "start_wf");

        var graphId = withIdempotencyKey(authLzyGrpcClient, "execute_graph")
            .executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(withMissingOutputSlot())
                .build())
            .getGraphId();

        finishWorkflow("workflow_1", workflow.getExecutionId(), "finish_wf");

        assertFalse(graphId.isBlank());
    }

    @Test
    public void repeatedOpsCollapseToSingle() {
        var workflow = startWorkflow("workflow_1", "start_wf");
        var countOfTasks = new AtomicInteger(0);

        graphExecutor().setOnExecute(request -> countOfTasks.addAndGet(request.getTasksCount()));

        var graphId = withIdempotencyKey(authLzyGrpcClient, "execute_graph")
            .executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName("workflow_1")
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphWithRepeatedOps())
                .build())
            .getGraphId();

        finishWorkflow("workflow_1", workflow.getExecutionId(), "finish_wf");

        assertSame(2, countOfTasks.get());
    }

    @Test
    public void executeGraphWithSingleProducerMultipleConsumers() {
        var workflowName = "workflow_1";
        var workflow = startWorkflow(workflowName, "start_wf");

        var graphs = producerAndConsumersGraphs();

        var graphId1 = withIdempotencyKey(authLzyGrpcClient, "execute_graph_1").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(0))
                .build()).getGraphId();

        var graphId2 = withIdempotencyKey(authLzyGrpcClient, "execute_graph_2").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(1))
                .build()).getGraphId();

        var graphId3 = withIdempotencyKey(authLzyGrpcClient, "execute_graph_3").executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(workflow.getExecutionId())
                .setGraph(graphs.get(2))
                .build()).getGraphId();

        assertFalse(graphId1.isBlank());
        assertFalse(graphId2.isBlank());
        assertFalse(graphId3.isBlank());
    }
}
