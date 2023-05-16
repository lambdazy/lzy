package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.GraphDao;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

final class StopGraphs extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final GraphExecutorBlockingStub graphExecutorClient;

    public StopGraphs(ExecutionStepContext stepCtx, StopExecutionState state,
                      GraphExecutorBlockingStub graphExecutorClient)
    {
        super(stepCtx, state);
        this.graphExecutorClient = graphExecutorClient;
    }

    @Override
    public StepResult get() {
        log().info("{} Stop executed graphs", logPrefix());

        final List<GraphDao.GraphDescription> graphIds;
        try {
            graphIds = withRetries(log(), () -> graphDao().getAll(execId()));
        } catch (Exception e) {
            return retryableFail(e, "Error while getting executed graphs from dao", Status.INTERNAL
                .withDescription("Cannot stop executed graphs").asRuntimeException());
        }

        try {
            //noinspection ResultOfMethodCallIgnored
            graphIds.forEach(graph -> graphExecutorClient.stop(GraphExecutorApi.GraphStopRequest.newBuilder()
                .setWorkflowId(graph.executionId()).setGraphId(graph.graphId()).build()));
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error while GraphExecutorBlockingStub::stopGraph call", sre);
        }

        return StepResult.CONTINUE;
    }
}
