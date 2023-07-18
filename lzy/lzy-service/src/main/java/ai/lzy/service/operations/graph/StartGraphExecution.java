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

final class StartGraphExecution extends ExecuteGraphContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final GraphExecutorBlockingStub graphExecutorClient;
    private final String allocatorSessionId;

    public StartGraphExecution(ExecutionStepContext stepCtx, ExecuteGraphState state,
                               GraphExecutorBlockingStub graphExecutorClient, String allocatorSessionId)
    {
        super(stepCtx, state);
        this.graphExecutorClient = graphExecutorClient;
        this.allocatorSessionId = allocatorSessionId;
    }

    @Override
    public StepResult get() {
        if (graphId() != null) {
            log().debug("{} Graph already executed, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        GraphExecuteRequest.Builder builder = GraphExecuteRequest.newBuilder()
            .setUserId(userId())
            .setWorkflowId(execId())
            .setWorkflowName(wfName())
            .setAllocatorSessionId(allocatorSessionId)
            .setParentGraphId(request().getParentGraphId())
            .addAllTasks(tasks())
            .addAllChannels(channels().values().stream().map(channelId -> GraphExecutor.ChannelDesc.newBuilder()
                .setId(channelId)
                .setDirect(GraphExecutor.ChannelDesc.DirectChannel.getDefaultInstance())
                .build()
            ).toList());

        log().info("{} Request to execute graph to GraphExecutor service...", logPrefix());

        final String graphId;
        try {
            graphId = graphExecutorClient.execute(builder.build()).getStatus().getGraphId();
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error while GraphExecutor::execute call", sre);
        }

        log().debug("{} GraphExecutor is successfully requested. Save graph id to dao...", logPrefix());
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
