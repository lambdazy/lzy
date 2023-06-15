package ai.lzy.service.operations.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.config.AllocatorSessionSpec;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.CreateSessionResponse;
import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class CreateAllocatorSession extends StartExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final AllocatorSessionSpec spec;
    private final AllocatorBlockingStub allocClient;

    public CreateAllocatorSession(ExecutionStepContext stepCtx, StartExecutionState state,
                                  AllocatorSessionSpec spec, AllocatorBlockingStub allocClient)
    {
        super(stepCtx, state);
        this.spec = spec;
        this.allocClient = allocClient;
    }

    @Override
    public StepResult get() {
        if (allocatorSessionId() != null) {
            log().debug("{} Allocator session already created, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Create allocator session...", logPrefix());

        var createSessionAllocClient = (idempotencyKey() == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey() + "_alloc_session");
        final LongRunning.Operation createSessionOp;

        try {
            createSessionOp = createSessionAllocClient.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(spec.userId())
                    .setDescription(spec.description())
                    .setCachePolicy(spec.cachePolicy())
                    .build());
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in Allocator:createSession call", sre);
        }

        final String sessionId;

        if (createSessionOp.getDone() && createSessionOp.hasResponse()) {
            try {
                sessionId = createSessionOp.getResponse().unpack(CreateSessionResponse.class).getSessionId();
            } catch (InvalidProtocolBufferException e) {
                log().error("{} Cannot parse CreateSessionResponse of operation with id='{}': {}", logPrefix(),
                    createSessionOp.getId(), e.getMessage(), e);
                return StepResult.RESTART;
            }
        } else {
            var status = (createSessionOp.getDone() && createSessionOp.hasError())
                ? StatusProto.toStatusRuntimeException(createSessionOp.getError())
                : Status.INTERNAL.withDescription("operation must be completed").asRuntimeException();
            log().error("{} CreateSession operation with id='{}' in invalid state: {}", logPrefix(),
                createSessionOp.getId(), status.getMessage());

            return failAction().apply(status);
        }

        log().debug("{} Allocator session with id='{}' successfully created", logPrefix(), sessionId);

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
            return retryableFail(e, "Cannot save data about allocator session with id='%s'".formatted(sessionId),
                deleteSession, Status.INTERNAL.withDescription("Cannot create allocator session").asRuntimeException());
        }

        setAllocatorSessionId(sessionId);
        log().debug("{} Allocator session successfully created...", logPrefix());

        return StepResult.CONTINUE;
    }
}
