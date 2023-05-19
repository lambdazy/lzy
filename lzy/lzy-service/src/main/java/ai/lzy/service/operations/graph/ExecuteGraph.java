package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.graph.GraphExecutor;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecuteRequest;
import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

public final class ExecuteGraph extends ExecuteGraphContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final GraphExecutorBlockingStub graphExecutorClient;

    public ExecuteGraph(ExecutionStepContext stepCtx, ExecuteGraphState state,
                        GraphExecutorBlockingStub graphExecutorClient)
    {
        super(stepCtx, state);
        this.graphExecutorClient = graphExecutorClient;
    }

    @Override
    public StepResult get() {
        if (graphId() != null) {
            log().debug("{} Graph already executed, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        GraphExecuteRequest.Builder builder = GraphExecuteRequest.newBuilder().setUserId(userId())
            .setWorkflowId(execId()).setWorkflowName(wfName())
            .setParentGraphId(request().getParentGraphId())
            .addAllTasks(tasks())
            .addAllChannels(channels().values().stream().map(channelId -> GraphExecutor.ChannelDesc.newBuilder()
                .setId(channelId)
                .setDirect(GraphExecutor.ChannelDesc.DirectChannel.getDefaultInstance())
                .build()
            ).toList());

        log().info("{} Send execute graph request to service", logPrefix());

        final String graphId;
        try {
            graphId = graphExecutorClient.execute(builder.build()).getStatus().getGraphId();
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error while GraphExecutor::execute call", sre);
        }

        log().debug("{} Save id of executed graph in dao...", logPrefix());
        setGraphId(graphId);

        try {
            saveState();
        } catch (Exception e) {
            return retryableFail(e, "Cannot save id of executed graph in dao", Status.INTERNAL
                .withDescription("Cannot execute graph").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
