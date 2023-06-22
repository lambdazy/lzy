package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

final class WaitDeleteAllocatorSession extends StopExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final LongRunningServiceBlockingStub allocOpService;

    public WaitDeleteAllocatorSession(ExecutionStepContext stepCtx, StopExecutionState state,
                                      LongRunningServiceBlockingStub allocOpService)
    {
        super(stepCtx, state);
        this.allocOpService = allocOpService;
    }

    @Override
    public StepResult get() {
        if (allocatorSessionId() == null) {
            log().debug("{} Allocator session id is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Test status of delete allocator session operation with id='{}'", logPrefix(),
            deleteAllocSessionOpId());

        final LongRunning.Operation op;
        try {
            op = allocOpService.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(deleteAllocSessionOpId()).build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in AllocOpService::get call for operation with id='%s'".formatted(
                deleteAllocSessionOpId()), sre);
        }

        if (!op.getDone()) {
            log().debug("{} Delete allocator session operation with id='{}' not completed yet, reschedule...",
                logPrefix(), op.getId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        try {
            withRetries(log(), () -> execDao().updateAllocatorSession(execId(), null, null));
        } catch (Exception e) {
            return retryableFail(e, "Cannot clean allocator session in dao", Status.INTERNAL.withDescription(
                "Cannot delete allocator session").asRuntimeException());
        }

        log().debug("{} Allocator session with id='{}' was successfully deleted", logPrefix(), allocatorSessionId());
        setAllocatorSessionId(null);

        return StepResult.CONTINUE;
    }
}
