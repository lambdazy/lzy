package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;

public class WaitDeleteAllocatorSession implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String execId;
    private final String opId;
    private final LongRunningServiceBlockingStub allocOpService;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public WaitDeleteAllocatorSession(ExecutionDao execDao, String execId, @Nullable String opId,
                                      LongRunningServiceBlockingStub allocOpService,
                                      Function<StatusRuntimeException, StepResult> failAction,
                                      Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.execId = execId;
        this.opId = opId;
        this.allocOpService = allocOpService;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        if (opId == null) {
            log.debug("{} Delete allocator session op id is null, skip step...", logPrefix);
            return StepResult.ALREADY_DONE;
        }

        log.info("{} Test status of delete allocator session operation: { opId: {} }", logPrefix, opId);

        final LongRunning.Operation op;
        try {
            op = allocOpService.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(opId).build());
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in AllocOpService::get call for operation with id='{}': {}", logPrefix, opId,
                sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        if (!op.getDone()) {
            log.debug("{} Delete allocator session operation with id='{}' not completed yet, reschedule...", logPrefix,
                op.getId());
            return StepResult.RESTART.after(Duration.ofMillis(500));
        }

        try {
            withRetries(log, () -> execDao.updateAllocatorSession(execId, null, null));
        } catch (Exception e) {
            log.error("{} Cannot clean allocator session in dao: {}", logPrefix, e.getMessage(), e);
            return failAction.apply(Status.INTERNAL.withDescription("Cannot delete allocator session")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
