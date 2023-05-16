package ai.lzy.service.workflow.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.Constants;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.workflow.ExecutionStepContext;
import ai.lzy.service.workflow.RetryableFailStep;
import ai.lzy.service.workflow.StartExecutionContextAwareStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class WaitAllocationPortalVm extends StartExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final AllocatorBlockingStub allocClient;
    private final LongRunningServiceBlockingStub allocOpClient;

    public WaitAllocationPortalVm(ExecutionStepContext stepCtx, StartExecutionState state,
                                  AllocatorBlockingStub allocClient, LongRunningServiceBlockingStub allocOpClient)
    {
        super(stepCtx, state);
        this.allocClient = allocClient;
        this.allocOpClient = allocOpClient;
    }

    @Override
    public StepResult get() {
        if (portalApiAddress() != null) {
            log().debug("{} Portal VM already allocated, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Test status of allocate portal VM operation: { opId: {} }", logPrefix(), allocateVmOpId());

        Function<Exception, Runnable> dropAllocVm = e -> () -> {
            try {
                //noinspection ResultOfMethodCallIgnored
                allocOpClient.cancel(LongRunning.CancelOperationRequest.newBuilder().setOperationId(allocateVmOpId())
                    .build());
            } catch (StatusRuntimeException sre) {
                log().warn("{} Cannot cancel allocate portal VM operation with id='{}' after error {}: ", logPrefix(),
                    allocateVmOpId(), e.getMessage(), sre);
            }

            var freeVmAllocClient = (idempotencyKey() == null) ? allocClient :
                withIdempotencyKey(allocClient, idempotencyKey() + "_free_portal_vm");
            try {
                //noinspection ResultOfMethodCallIgnored
                freeVmAllocClient.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(portalVmId()).build());
            } catch (StatusRuntimeException sre) {
                log().warn("{} Cannot free portal VM with id='{}' after error {}: ", logPrefix(), portalVmId(),
                    e.getMessage(), sre);
            }
        };

        final LongRunning.Operation op;
        try {
            op = allocOpClient.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(allocateVmOpId())
                .build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in AllocOpService::get call for operation with id='%s'".formatted(
                allocateVmOpId()), dropAllocVm.apply(sre), sre);
        }

        if (!op.getDone()) {
            log().debug("{} Allocate portal VM operation with id='{}' not completed yet, reschedule...", logPrefix(),
                allocateVmOpId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        VmAllocatorApi.AllocateResponse allocateResponse;
        if (op.hasResponse()) {
            try {
                allocateResponse = op.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
            } catch (InvalidProtocolBufferException e) {
                log().error("{} Cannot parse AllocateResponse from operation with id='{}': {}", logPrefix(),
                    allocateVmOpId(), e.getMessage(), e);
                return StepResult.RESTART;
            }

            try {
                withRetries(log(), () -> execDao().updatePortalAddresses(execId(),
                    allocateResponse.getMetadataOrDefault(Constants.PORTAL_ADDRESS_KEY, null),
                    allocateResponse.getMetadataOrDefault(Constants.FS_ADDRESS_KEY, null),
                    null
                ));
            } catch (Exception e) {
                return retryableFail(e, "Cannot save data about allocated portal VM", dropAllocVm.apply(e),
                    Status.INTERNAL.withDescription("Cannot allocate portal VM").asRuntimeException());
            }

            return StepResult.CONTINUE;
        }

        if (op.hasError()) {
            var status = StatusProto.toStatusRuntimeException(op.getError());
            log().error("{} Allocate portal VM operation with id='{}' completed with error: {}", logPrefix(),
                allocateVmOpId(), status.getMessage());
            return failAction().apply(status);
        }

        assert false;
        return StepResult.CONTINUE;
    }
}
