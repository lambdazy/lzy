package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.v1.portal.LzyPortalApi;
import ai.lzy.v1.portal.LzyPortalGrpc.LzyPortalBlockingStub;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class FinishPortal implements Supplier<StepResult> {
    private final String portalVmAddress;
    private final String idempotencyKey;
    private final LzyPortalBlockingStub portalClient;
    private final Consumer<String> opIdConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public FinishPortal(@Nullable String portalVmAddress, @Nullable String idempotencyKey,
                        LzyPortalBlockingStub portalClient, Consumer<String> opIdConsumer,
                        Function<StatusRuntimeException, StepResult> failAction,
                        Logger log, String logPrefix)
    {
        this.portalVmAddress = portalVmAddress;
        this.idempotencyKey = idempotencyKey;
        this.portalClient = portalClient;
        this.opIdConsumer = opIdConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        if (portalVmAddress == null) {
            log.debug("{} Portal VM address is null, skip step...", logPrefix);
            return StepResult.ALREADY_DONE;
        }

        log.info("{} Shutdown portal with VM address: {}", logPrefix, portalVmAddress);

        var finishPortalClient = (idempotencyKey == null) ? portalClient :
            withIdempotencyKey(portalClient, idempotencyKey + "_finish_portal");
        try {
            var shutdownPortalOp = finishPortalClient.finish(LzyPortalApi.FinishRequest.getDefaultInstance());
            opIdConsumer.accept(shutdownPortalOp.getId());
            return StepResult.CONTINUE;
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in PortalClient::finish call: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        }
    }
}
