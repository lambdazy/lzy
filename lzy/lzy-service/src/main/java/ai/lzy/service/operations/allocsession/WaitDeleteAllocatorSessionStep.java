package ai.lzy.service.operations.allocsession;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.DeleteAllocatorSessionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

final class WaitDeleteAllocatorSessionStep extends DeleteAllocatorSessionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final LongRunningServiceBlockingStub allocOpService;

    public WaitDeleteAllocatorSessionStep(ExecutionStepContext stepCtx, DeleteAllocatorSessionState state,
                                          LongRunningServiceBlockingStub allocOpService)
    {
        super(stepCtx, state);
        this.allocOpService = allocOpService;
    }

    @Override
    public StepResult get() {
        var deleteOpId = deleteSessionOpId();
        Objects.requireNonNull(deleteOpId, "%s %s failed, allocator op id is null"
            .formatted(logPrefix(), getClass().getName()));

        log().info("{} Test status of delete allocator session operation with id='{}'", logPrefix(), deleteOpId);

        final LongRunning.Operation op;
        try {
            op = allocOpService.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(deleteOpId).build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in AllocOpService::get call for operation with id='%s'".formatted(
                deleteOpId), sre);
        }

        if (!op.getDone()) {
            log().debug("{} Delete allocator session operation with id='{}' not completed yet, reschedule...",
                logPrefix(), op.getId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        log().debug("{} Allocator session with id='{}' was successfully deleted", logPrefix(), sessionId());
        return StepResult.CONTINUE;
    }
}
