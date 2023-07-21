package ai.lzy.service.operations.allocsession;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.db.DbHelper;
import ai.lzy.service.dao.DeleteAllocatorSessionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class DeleteAllocatorSessionStep extends DeleteAllocatorSessionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final AllocatorBlockingStub allocClient;

    public DeleteAllocatorSessionStep(ExecutionStepContext stepCtx, DeleteAllocatorSessionState state,
                                      AllocatorBlockingStub allocClient)
    {
        super(stepCtx, state);
        this.allocClient = allocClient;
    }

    @Override
    public StepResult get() {
        if (deleteSessionOpId() != null) {
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Delete allocator session with id='{}'", logPrefix(), sessionId());

        var deleteSessionClient = (idempotencyKey() == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey() + "_delete_session");

        final String deleteOpId;
        try {
            var op = deleteSessionClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId()).build());

            deleteOpId = op.getId();

            log().debug("{} Allocator session with id='{}' requested to delete, operationId='{}'", logPrefix(),
                sessionId(), deleteOpId);
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return StepResult.FINISH;
            }
            return retryableFail(sre, "Error in AllocatorGrpcClient::deleteSession call", sre);
        }

        try {
            DbHelper.withRetries(log(), () ->
                deleteAllocatorSessionOpsDao().setAllocatorOperationId(opId(), deleteOpId, null));
        } catch (Exception e) {
            return retryableFail(e, "Cannot set allocator operation id in dao", Status.INTERNAL.withDescription(
                "Cannot delete allocator session").asRuntimeException());
        }

        setDeleteSessionOpId(deleteOpId);

        return StepResult.CONTINUE;
    }
}
