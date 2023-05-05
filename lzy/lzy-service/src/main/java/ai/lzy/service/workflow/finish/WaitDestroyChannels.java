package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

public class WaitDestroyChannels implements Supplier<StepResult> {
    private final String opId;
    private final LongRunningServiceBlockingStub channelsOpService;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public WaitDestroyChannels(@Nullable String opId, LongRunningServiceBlockingStub channelsOpService,
                               Function<StatusRuntimeException, StepResult> failAction, Logger log, String logPrefix)
    {
        this.opId = opId;
        this.channelsOpService = channelsOpService;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        if (opId == null) {
            log.debug("{} Destroy channels op id is null, skip step...", logPrefix);
            return StepResult.ALREADY_DONE;
        }

        log.info("{} Test status of destroy channels operation: { opId: {} }", logPrefix, opId);

        final LongRunning.Operation op;
        try {
            op = channelsOpService.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(opId).build());
        } catch (StatusRuntimeException sre) {
            log.error("{} Error while getting destroy channels operation with id='{}': {}", logPrefix, opId,
                sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        if (!op.getDone()) {
            log.debug("{} Destroy channels operation with id='{}' not completed yet, reschedule...", logPrefix,
                op.getId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        return StepResult.CONTINUE;
    }
}
