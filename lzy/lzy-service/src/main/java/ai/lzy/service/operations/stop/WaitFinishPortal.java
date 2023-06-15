package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;
import static ai.lzy.v1.longrunning.LongRunningServiceGrpc.newBlockingStub;

final class WaitFinishPortal extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    public WaitFinishPortal(ExecutionStepContext stepCtx, StopExecutionState state) {
        super(stepCtx, state);
    }

    @Override
    public StepResult get() {
        if (portalApiAddress() == null) {
            log().debug("{} Portal VM address is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Test status of shutdown portal operation: { opId: {} }", logPrefix(), finishPortalOpId());

        var grpcChannel = newGrpcChannel(portalApiAddress(), LongRunningServiceGrpc.SERVICE_NAME);

        try {
            var portalOpClient = newBlockingClient(newBlockingStub(grpcChannel), APP,
                () -> internalUserCredentials().get().token());

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
        } finally {
            grpcChannel.shutdown();
            try {
                grpcChannel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // intentionally blank
            } finally {
                grpcChannel.shutdownNow();
            }
        }

        setPortalApiAddress(null);
        return StepResult.CONTINUE;
    }
}
