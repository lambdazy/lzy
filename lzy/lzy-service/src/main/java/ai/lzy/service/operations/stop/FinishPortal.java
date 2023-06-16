package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static ai.lzy.service.LzyService.APP;
import static ai.lzy.util.grpc.GrpcUtils.*;
import static ai.lzy.v1.portal.LzyPortalGrpc.newBlockingStub;

final class FinishPortal extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    public FinishPortal(ExecutionStepContext stepCtx, StopExecutionState state) {
        super(stepCtx, state);
    }

    @Override
    public StepResult get() {
        if (portalApiAddress() == null) {
            log().debug("{} Portal VM address is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Shutdown portal running at VM with address='{}'", logPrefix(), portalApiAddress());

        var grpcChannel = newGrpcChannel(portalApiAddress(), LzyPortalGrpc.SERVICE_NAME);

        try {
            var portalClient = newBlockingClient(newBlockingStub(grpcChannel), APP,
                () -> internalUserCredentials().get().token());
            var finishPortalClient = (idempotencyKey() == null) ? portalClient :
                withIdempotencyKey(portalClient, idempotencyKey() + "_finish_portal");
            try {
                var op = finishPortalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());

                setFinishPortalOpId(op.getId());
                log().debug("{} Portal shutdown operation with id='{}' starts...", logPrefix(), op.getId());

                return StepResult.CONTINUE;
            } catch (StatusRuntimeException sre) {
                return retryableFail(sre, "Error in PortalClient::finish call", sre);
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
    }
}
