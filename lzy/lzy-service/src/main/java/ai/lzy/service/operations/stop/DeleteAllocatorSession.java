package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class DeleteAllocatorSession extends StopExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final AllocatorBlockingStub allocClient;

    public DeleteAllocatorSession(ExecutionStepContext stepCtx, StopExecutionState state,
                                  AllocatorBlockingStub allocClient)
    {
        super(stepCtx, state);
        this.allocClient = allocClient;
    }

    @Override
    public StepResult get() {
        if (allocatorSessionId() == null) {
            log().debug("{} Allocator session id is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Delete allocator session with id='{}'", logPrefix(), allocatorSessionId());

        var deleteSessionClient = (idempotencyKey() == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey() + "_delete_session");
        try {
            var op = deleteSessionClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(allocatorSessionId()).build());

            setDeleteAllocSessionOpId(op.getId());
            log().debug("{} Allocator session with id='{}' requested to delete, operationId='{}'", logPrefix(),
                allocatorSessionId(), op.getId());

            return StepResult.CONTINUE;
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in AllocatorGrpcClient::deleteSession call", sre);
        }
    }
}
