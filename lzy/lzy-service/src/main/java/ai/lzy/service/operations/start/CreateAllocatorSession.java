package ai.lzy.service.operations.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.CreateSessionResponse;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.time.Duration;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class CreateAllocatorSession extends StartExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final Duration allocatorVmCacheTimeout;
    private final AllocatorBlockingStub allocClient;

    public CreateAllocatorSession(ExecutionStepContext stepCtx, StartExecutionState state,
                                  Duration allocatorVmCacheTimeout, AllocatorBlockingStub allocClient)
    {
        super(stepCtx, state);
        this.allocatorVmCacheTimeout = allocatorVmCacheTimeout;
        this.allocClient = allocClient;
    }

    @Override
    public StepResult get() {
        if (allocatorSessionId() != null) {
            log().debug("{} Allocator session already created, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Create allocator session: { userId: {}, wfName: {}, execId: {} }", logPrefix(), userId(),
            wfName(), execId());

        var createSessionAllocClient = (idempotencyKey() == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey() + "_alloc_session");
        final LongRunning.Operation createSessionOp;

        try {
            createSessionOp = createSessionAllocClient.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(userId())
                    .setDescription("session for exec with id=" + execId())
                    .setCachePolicy(VmAllocatorApi.CachePolicy.newBuilder()
                        .setIdleTimeout(Durations.fromSeconds(allocatorVmCacheTimeout.getSeconds()))
                        .build())
                    .build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in Allocator:createSession call for execution with id='%s'"
                .formatted(execId()), () -> {}, sre);
        }

        final String sessionId;

        if (createSessionOp.getDone() && createSessionOp.hasResponse()) {
            try {
                sessionId = createSessionOp.getResponse().unpack(CreateSessionResponse.class).getSessionId();
            } catch (InvalidProtocolBufferException e) {
                log().error("{} Cannot parse CreateSessionResponse from operation with id='{}': {}", logPrefix(),
                    createSessionOp.getId(), e.getMessage(), e);
                return StepResult.RESTART;
            }
        } else {
            var status = (createSessionOp.getDone() && createSessionOp.hasError())
                ? StatusProto.toStatusRuntimeException(createSessionOp.getError())
                : Status.INTERNAL.withDescription("operation must be completed").asRuntimeException();
            log().error("{} Create session operation with id='{}' in invalid state: {}", logPrefix(),
                createSessionOp.getId(), status.getMessage());

            return failAction().apply(status);
        }

        log().debug("{} Allocator session successfully created: { sessionId: {} }", logPrefix(), sessionId);

        try {
            withRetries(log(), () -> execDao().updateAllocatorSession(execId(), sessionId, null));
        } catch (Exception e) {
            Runnable deleteSession = () -> {
                try {
                    var deleteSessionAllocClient = (idempotencyKey() == null) ? allocClient :
                        withIdempotencyKey(allocClient, idempotencyKey() + "_delete_session");
                    //noinspection ResultOfMethodCallIgnored
                    deleteSessionAllocClient.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder()
                        .setSessionId(sessionId).build());
                } catch (StatusRuntimeException sre) {
                    log().warn("{} Cannot delete allocator session with id='{}' after error {}: ", logPrefix(),
                        sessionId, e.getMessage(), sre);
                }
            };
            return retryableFail(e, "Cannot save data about allocator session", deleteSession, Status.INTERNAL
                .withDescription("Cannot create allocator session").asRuntimeException());
        }

        setAllocatorSessionId(sessionId);
        return StepResult.CONTINUE;
    }
}
