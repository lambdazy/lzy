package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.function.Supplier;

final class WaitDestroyChannels extends StopExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final LongRunningServiceBlockingStub channelsOpService;

    public WaitDestroyChannels(ExecutionStepContext stepCtx, StopExecutionState state,
                               LongRunningServiceBlockingStub channelsOpService)
    {
        super(stepCtx, state);
        this.channelsOpService = channelsOpService;
    }

    @Override
    public StepResult get() {
        log().info("{} Test status of destroy channels operation: { opId: {} }", logPrefix(), destroyChannelsOpId());

        final LongRunning.Operation op;
        try {
            op = channelsOpService.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(destroyChannelsOpId()).build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error while getting destroy channels operation with id='%s'"
                .formatted(destroyChannelsOpId()), sre);
        }

        if (!op.getDone()) {
            log().debug("{} Destroy channels operation with id='{}' not completed yet, reschedule...", logPrefix(),
                op.getId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        return StepResult.CONTINUE;
    }
}
