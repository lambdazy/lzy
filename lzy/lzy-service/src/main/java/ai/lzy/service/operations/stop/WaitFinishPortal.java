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

final class WaitFinishPortal extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final LongRunningServiceBlockingStub portalOpClient;

    public WaitFinishPortal(ExecutionStepContext stepCtx, StopExecutionState state,
                            LongRunningServiceBlockingStub portalOpClient)
    {
        super(stepCtx, state);
        this.portalOpClient = portalOpClient;
    }

    @Override
    public StepResult get() {
        if (portalApiAddress() == null) {
            log().debug("{} Portal VM address is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Test status of shutdown portal operation: { opId: {} }", logPrefix(), finishPortalOpId());

        final LongRunning.Operation op;
        try {
            op = portalOpClient.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(finishPortalOpId())
                .build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Cannot get shutdown portal operation with id='%s'".formatted(
                finishPortalOpId()), sre);
        }

        if (!op.getDone()) {
            log().debug("{} Shutdown portal operation with id='{}' not completed yet, reschedule...", logPrefix(),
                op.getId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        try {
            withRetries(log(), () -> execDao().updatePortalAddresses(execId(), null, null, null));
        } catch (Exception e) {
            return retryableFail(e, "Cannot clean portal VM address in dao", Status.INTERNAL.withDescription(
                "Cannot finish portal").asRuntimeException());
        }

        setPortalApiAddress(null);
        return StepResult.CONTINUE;
    }
}
