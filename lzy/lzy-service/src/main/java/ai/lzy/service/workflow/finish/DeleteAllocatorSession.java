package ai.lzy.service.workflow.finish;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class DeleteAllocatorSession implements Supplier<StepResult> {
    private final String sessionId;
    private final String idempotencyKey;
    private final AllocatorBlockingStub allocClient;
    private final Consumer<String> resultConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public DeleteAllocatorSession(@Nullable String sessionId, @Nullable String idempotencyKey,
                                  AllocatorBlockingStub allocClient, Consumer<String> resultConsumer,
                                  Function<StatusRuntimeException, StepResult> failAction,
                                  Logger log, String logPrefix)
    {
        this.sessionId = sessionId;
        this.idempotencyKey = idempotencyKey;
        this.allocClient = allocClient;
        this.resultConsumer = resultConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        if (sessionId == null) {
            log.debug("{} Allocator session id is null, skip step...", logPrefix);
            return StepResult.ALREADY_DONE;
        }

        log.info("{} Delete allocator session: { sessionId: {} }", logPrefix, sessionId);

        var deleteSessionClient = (idempotencyKey == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey + "_delete_session");
        try {
            var op = deleteSessionClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                .setSessionId(sessionId).build());
            resultConsumer.accept(op.getId());
            return StepResult.CONTINUE;
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in AllocClient::deleteSession call: { sessionId: {}, error: {} }", logPrefix, sessionId,
                sre.getMessage(), sre);
            return failAction.apply(sre);
        }
    }
}
