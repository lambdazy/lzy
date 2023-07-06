package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class DestroyChannels extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final LzyChannelManagerPrivateBlockingStub channelsClient;

    public DestroyChannels(ExecutionStepContext stepCtx, StopExecutionState state,
                           LzyChannelManagerPrivateBlockingStub channelsClient)
    {
        super(stepCtx, state);
        this.channelsClient = channelsClient;
    }

    @Override
    public StepResult get() {
        log().info("{} Destroy channels of execution...", logPrefix());

        var destroyAllChannelsClient = (idempotencyKey() == null) ? channelsClient :
            withIdempotencyKey(channelsClient, idempotencyKey() + "_destroy_exec_channels");
        try {
            destroyAllChannelsClient.destroyAll(LCMPS.DestroyAllRequest.newBuilder()
                .setExecutionId(execId()).build());
            log().debug("{} Channels destroyed", logPrefix());

            return StepResult.CONTINUE;
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in Channels::destroyAll call for execution with id='%s'"
                .formatted(execId()), sre);
        }
    }
}
