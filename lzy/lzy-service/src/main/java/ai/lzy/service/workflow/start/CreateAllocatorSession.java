package ai.lzy.service.workflow.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.CreateSessionResponse;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class CreateAllocatorSession implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String userId;
    private final String wfName;
    private final String execId;
    private final Duration allocatorVmCacheTimeout;
    private final String idempotencyKey;
    private final AllocatorBlockingStub allocClient;
    private final Consumer<String> sessionIdConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public CreateAllocatorSession(ExecutionDao execDao, String userId, String wfName, String execId,
                                  Duration allocatorVmCacheTimeout, @Nullable String idempotencyKey,
                                  AllocatorBlockingStub allocClient, Consumer<String> sessionIdConsumer,
                                  Function<StatusRuntimeException, StepResult> failAction,
                                  Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.userId = userId;
        this.wfName = wfName;
        this.execId = execId;
        this.allocatorVmCacheTimeout = allocatorVmCacheTimeout;
        this.idempotencyKey = idempotencyKey;
        this.allocClient = allocClient;
        this.sessionIdConsumer = sessionIdConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Create allocator session: { userId: {}, wfName: {}, execId: {} }", logPrefix, userId, wfName,
            execId);

        var createSessionAllocClient = (idempotencyKey == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey + "_alloc_session");
        final LongRunning.Operation createSessionOp;

        try {
            createSessionOp = createSessionAllocClient.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(userId)
                    .setDescription("session for exec with id=" + execId)
                    .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder()
                        .setIdleTimeout(Durations.fromSeconds(allocatorVmCacheTimeout.getSeconds()))
                        .build())
                    .build());
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in Allocator:createSession call for execution with id='{}': {}", logPrefix, execId,
                sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        final String sessionId;

        if (createSessionOp.getDone() && createSessionOp.hasResponse()) {
            try {
                sessionId = createSessionOp.getResponse().unpack(CreateSessionResponse.class).getSessionId();
                sessionIdConsumer.accept(sessionId);
            } catch (InvalidProtocolBufferException e) {
                log.error("{} Cannot parse CreateSessionResponse from operation with id='{}': {}", logPrefix,
                    createSessionOp.getId(), e.getMessage(), e);
                return StepResult.RESTART;
            }
        } else {
            var status = (createSessionOp.getDone() && createSessionOp.hasError())
                ? StatusProto.toStatusRuntimeException(createSessionOp.getError())
                : Status.INTERNAL.withDescription("operation must be completed").asRuntimeException();
            log.error("{} Create session operation with id='{}' in invalid state: {}", logPrefix,
                createSessionOp.getId(), status.getMessage());

            return failAction.apply(status);
        }

        log.debug("{} Allocator session successfully created: { sessionId: {} }", logPrefix, sessionId);

        try {
            withRetries(log, () -> execDao.updateAllocatorSession(execId, sessionId, null));
        } catch (Exception e) {
            log.error("{} Cannot save data about allocator session: {}", logPrefix, e.getMessage(), e);
            try {
                var deleteSessionAllocClient = (idempotencyKey == null) ? allocClient :
                    withIdempotencyKey(allocClient, idempotencyKey + "_delete_session");
                //noinspection ResultOfMethodCallIgnored
                deleteSessionAllocClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                    .setSessionId(sessionId).build());
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot delete allocator session with id='{}' after error {}: ", logPrefix, sessionId,
                    e.getMessage(), sre);
            }
            return failAction.apply(Status.INTERNAL.withDescription("Cannot create allocator session")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
