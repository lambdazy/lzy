package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class FreePortalVm extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final AllocatorBlockingStub allocClient;

    public FreePortalVm(ExecutionStepContext stepCtx, StopExecutionState state, AllocatorBlockingStub allocClient) {
        super(stepCtx, state);
        this.allocClient = allocClient;
    }

    @Override
    public StepResult get() {
        if (portalVmId() == null) {
            log().debug("{} Portal VM id is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Free portal VM: { vmId: {} }", logPrefix(), portalVmId());

        var freeVmAllocClient = (idempotencyKey() == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey() + "_free_portal_vm");
        try {
            //noinspection ResultOfMethodCallIgnored
            freeVmAllocClient.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(portalVmId()).build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in AllocatorGrpcClient:free call for VM with id='%s'"
                .formatted(portalVmId()), sre);
        }

        try {
            withRetries(log(), () -> execDao().updateAllocateOperationData(execId(), null, null, null));
        } catch (Exception e) {
            return retryableFail(e, "Cannot clean portal VM id in dao", Status.INTERNAL.withDescription(
                "Cannot free portal VM").asRuntimeException());
        }

        setPortalVmId(null);
        return StepResult.CONTINUE;
    }
}
