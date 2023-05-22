package ai.lzy.service.operations.stop;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalBlockingStub;
import io.grpc.StatusRuntimeException;

import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class FinishPortal extends StopExecutionContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final LzyPortalBlockingStub portalClient;

    public FinishPortal(ExecutionStepContext stepCtx, StopExecutionState state, LzyPortalBlockingStub portalClient) {
        super(stepCtx, state);
        this.portalClient = portalClient;
    }

    @Override
    public StepResult get() {
        if (portalApiAddress() == null) {
            log().debug("{} Portal VM address is null, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Shutdown portal running at VM with address='{}'", logPrefix(), portalApiAddress());

        var finishPortalClient = (idempotencyKey() == null) ? portalClient :
            withIdempotencyKey(portalClient, idempotencyKey() + "_finish_portal");
        try {
            var op = finishPortalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());
            setFinishPortalOpId(op.getId());
            return StepResult.CONTINUE;
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in PortalClient::finish call", sre);
        }
    }
}
