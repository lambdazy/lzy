package ai.lzy.service.operations.stop;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.util.auth.exceptions.AuthException;
import io.grpc.Status;

import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

final class DeletePortalSubject extends StopExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final SubjectServiceGrpcClient subjClient;

    public DeletePortalSubject(ExecutionStepContext stepCtx, StopExecutionState state,
                               SubjectServiceGrpcClient subjClient)
    {
        super(stepCtx, state);
        this.subjClient = subjClient;
    }

    @Override
    public StepResult get() {
        if (portalSubjectId() == null) {
            log().debug("{} Portal subject id is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Delete portal iam subject with id='{}'", logPrefix(), portalSubjectId());

        try {
            subjClient.removeSubject(portalSubjectId());
        } catch (AuthException e) {
            return retryableFail(e, "Error while deleting portal subject", Status.INTERNAL.withDescription(
                "Cannot delete portal subject").asRuntimeException());
        }

        try {
            withRetries(log(), () -> {
                try (var tx = TransactionHandle.create(storage())) {
                    execDao().updatePortalId(execId(), null, tx);
                    execDao().updatePortalSubjectId(execId(), null, tx);
                }
            });
        } catch (Exception e) {
            return retryableFail(e, "Cannot delete portal ids in dao", Status.INTERNAL.withDescription(
                "Cannot delete portal subject").asRuntimeException());
        }

        log().debug("{} Portal iam subject with id='{}' successfully deleted", logPrefix(), portalSubjectId());
        setPortalSubjectId(null);

        return StepResult.CONTINUE;
    }
}
