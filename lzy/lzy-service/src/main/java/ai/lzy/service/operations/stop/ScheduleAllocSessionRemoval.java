package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

class ScheduleAllocSessionRemoval extends StopExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final Duration delay;

    public ScheduleAllocSessionRemoval(ExecutionStepContext stepCtx, StopExecutionState state, Duration delay) {
        super(stepCtx, state);
        this.delay = delay;
    }

    @Override
    public StepResult get() {
        var sid = allocatorSessionId();
        if (sid == null) {
            return StepResult.ALREADY_DONE;
        }

        try {
            var deadline = Instant.now().plus(delay);
            withRetries(log(), () -> wfDao().releaseAllocatorSession(userId(), wfName(), sid, deadline));
            cleanAllocatorSessionId();
        } catch (Exception e) {
            log().error("{} Cannot release allocator session {} for workflow { userId: {}, wfName: {} }: {}",
                logPrefix(), sid, userId(), wfName(), e.getMessage());
            return StepResult.RESTART;
        }

        return StepResult.CONTINUE;
    }
}
