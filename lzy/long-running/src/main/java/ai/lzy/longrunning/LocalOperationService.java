package ai.lzy.longrunning;

import ai.lzy.util.grpc.ContextAwareTask;
import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunning.CancelOperationRequest;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class LocalOperationService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LocalOperationService.class);

    private final String name;

    private final Map<String, OperationDesc> operations = new ConcurrentHashMap<>();
    private final Map<String, Operation> idempotencyKey2Operation = new ConcurrentHashMap<>();

    /**
     * @param name Service name for logs
     */
    public LocalOperationService(String name) {
        this.name = name;
    }

    /**
     * Atomic upsert operation. Returns immutable copy of presented operation,
     * which should be considered as operation's state snapshot.
     */
    public OperationSnapshot registerOperation(Operation operation) {
        return registerOperation(operation, null);
    }

    private OperationSnapshot registerOperation(Operation operation, @Nullable Thread thread) {
        LOG.debug("[{}] Attempt to register operation {}", name, operation.toShortString());

        Operation.IdempotencyKey idempotencyKey = operation.idempotencyKey();

        if (idempotencyKey != null) {
            var currentAssocOp = idempotencyKey2Operation.computeIfAbsent(idempotencyKey.token(), ik -> {
                operations.putIfAbsent(operation.id(), new OperationDesc(operation, thread));
                return operation;
            });

            if (!currentAssocOp.id().equals(operation.id())) {
                LOG.debug("[{}] Found operation by idempotency key: { opId: {}, key: {} }", name,
                    currentAssocOp.id(), idempotencyKey.token());
            }

            synchronized (currentAssocOp.id()) {
                return OperationSnapshot.of(currentAssocOp);
            }
        }

        LOG.info("[{}] Register new operation {}", name, operation.toShortString());
        var op = operations.computeIfAbsent(operation.id(), id -> new OperationDesc(operation, thread));

        if (!op.operation.id().equals(operation.id())) {
            LOG.warn("[{}] Operation with id {} already exists.", name, operation.id());
        }

        return OperationSnapshot.of(op.operation);
    }

    /**
     * Execute operation in new Thread and complete it on response or error
     */
    public OperationSnapshot execute(Operation op, Supplier<Message> runnable) {
        return execute(op, runnable, Map.of(), Map.of());
    }

    public OperationSnapshot execute(Operation op, Supplier<Message> runnable, Map<String, String> logContextOverrides,
                                     Map<Metadata.Key<String>, String> grpcHeadersOverrides)
    {
        var task = new ContextAwareTask() {
            @Override
            protected Map<String, String> prepareLogContext() {
                return logContextOverrides;
            }

            @Override
            protected Map<Metadata.Key<String>, String> prepareGrpcHeaders() {
                return grpcHeadersOverrides;
            }

            @Override
            protected void execute() {
                try {
                    final var response =  runnable.get();
                    updateResponse(op.id(), response);
                } catch (StatusRuntimeException e) {
                    LOG.error("Error while executing op {}: ", op.id(), e);
                    updateError(op.id(), e.getStatus());
                } catch (Exception e) {
                    LOG.error("Error while executing op {}: ", op.id(), e);
                    updateError(op.id(), Status.INTERNAL.withDescription(e.getMessage()));
                }
            }
        };

        final var thread = new Thread(null, task, "operation-" + op.id());

        thread.start();
        return registerOperation(op, thread);
    }

    /**
     * Atomic get operation's state snapshot by idempotency key.
     */
    @Nullable
    public OperationSnapshot getByIdempotencyKey(String key) {
        var op = idempotencyKey2Operation.get(key);
        if (op != null) {
            LOG.debug("[{}] Got operation by idempotency key: { key: {}, opId: {} }", name, key, op.id());
            synchronized (op.id()) {
                return OperationSnapshot.of(op);
            }
        }
        LOG.debug("[{}] Operation with idempotency key not found: { key: {} }", name, key);
        return null;
    }

    /**
     * Atomic update operation. Returns updated operation's state snapshot.
     */
    @Nullable
    public OperationSnapshot updateResponse(String opId, Message response) {
        var op = operations.get(opId);
        if (op != null) {
            LOG.info("[{}] Update operation response: { opId: {} }", name, opId);

            synchronized (op.operation.id()) {
                op.operation.completeWith(response);
                op.operation.id().notifyAll();
                return OperationSnapshot.of(op.operation);
            }
        }
        LOG.error("[{}] Operation not found: { opId: {} }", name, opId);
        return null;
    }

    /**
     * Atomic update operation. Returns updated operation's state snapshot.
     */
    @Nullable
    public OperationSnapshot updateError(String opId, Status error) {
        var op = operations.get(opId);
        if (op != null) {
            LOG.info("[{}] Update operation error: { opId: {} }", name, opId);

            synchronized (op.operation.id()) {
                op.operation.completeWith(error);
                op.operation.id().notifyAll();
                return OperationSnapshot.of(op.operation);
            }
        }
        LOG.error("[{}] Operation not found: { opId: {} }", name, opId);
        return null;
    }

    @Nullable
    public OperationSnapshot get(String opId) {
        var op = operations.get(opId);
        if (op != null) {
            LOG.debug("[{}] Got operation: { opId: {} }", name, opId);

            synchronized (op.operation.id()) {
                return OperationSnapshot.of(op.operation);
            }
        }
        LOG.error("[{}] Operation not found: { opId: {} }", name, opId);
        return null;
    }

    @Override
    public void get(LongRunning.GetOperationRequest request, StreamObserver<LongRunning.Operation> response) {
        var op = operations.get(request.getOperationId());
        if (op == null) {
            var errorMessage = "Operation %s not found".formatted(request.getOperationId());
            LOG.error("[{}] Got error: {}", name, errorMessage);
            response.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
            return;
        }

        LongRunning.Operation protoOp;

        synchronized (op.operation.id()) {
            if (op.operation.done()) {
                if (op.operation.response() != null) {
                    LOG.info("[{}] Got operation is successfully completed: { opId: {} }", name, op.operation.id());
                } else if (op.operation.error() != null) {
                    LOG.info("[{}] Got operation is failed: { opId: {}, error: {} }", name, op.operation.id(),
                        op.operation.error());
                } else {
                    LOG.error("[{}] Got completed operation is in unknown state: { opId: {}, state: {} }", name,
                        op.operation.id(), op.toString());
                }
            } else {
                LOG.debug("[{}] Got operation is in progress: { opId: {} }", name, op.operation.id());
            }
            protoOp = op.operation.toProto();
        }

        response.onNext(protoOp);
        response.onCompleted();
    }

    @Nullable
    public OperationSnapshot cancel(String opId, String reason) {
        var op = operations.get(opId);
        if (op != null) {
            LOG.debug("[{}] Got operation: { opId: {} }", name, opId);

            synchronized (op.operation.id()) {
                op.operation.completeWith(Status.CANCELLED.withDescription(reason));
                op.operation.id().notifyAll();

                if (op.thread != null) {
                    op.thread.interrupt();
                }

                return OperationSnapshot.of(op.operation);
            }
        }
        LOG.error("[{}] Operation not found: { opId: {} }", name, opId);
        return null;
    }

    @Override
    public void cancel(CancelOperationRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("Cancelling operation {} with message {}", request.getOperationId(), request.getMessage());

        var op = operations.get(request.getOperationId());
        if (op == null) {
            var errorMessage = "Operation %s not found".formatted(request.getOperationId());
            LOG.error("[{}] Got error: {}", name, errorMessage);
            response.onError(Status.NOT_FOUND.withDescription(errorMessage).asException());
            return;
        }

        synchronized (op.operation.id()) {
            op.operation.completeWith(Status.CANCELLED.withDescription(request.getMessage()));
            op.operation.id().notifyAll();

            if (op.thread != null) {
                op.thread.interrupt();
            }
        }

        LOG.info(" Operation {} cancelled", request.getOperationId());
        response.onNext(op.operation.toProto());
        response.onCompleted();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean await(String opId, Duration timeout) {
        var nanos = timeout.toNanos();
        var deadline = System.nanoTime() + nanos;

        var op = operations.get(opId);
        if (op == null) {
            LOG.error("[{}] Operation not found: { opId: {} }", name, opId);
            return false;
        }

        var waited = false;
        try {
            synchronized (op.operation.id()) {
                while (!op.operation.done() && (nanos = deadline - System.nanoTime()) > 0L) {
                    op.operation.id().wait(Duration.ofNanos(nanos).toMillis());
                }
                waited = op.operation.done();
            }
        } catch (InterruptedException e) {
            LOG.warn("[{}] Was interrupted while waiting operation completed: { opId: {}, error: {} }", name, opId,
                e.getMessage(), e);
        }

        if (!waited) {
            LOG.error("[{}] Waiting deadline exceeded: { opId: {} }", name, opId);
        }

        return waited;
    }

    public record OperationSnapshot(String id, String createdBy, Instant createdAt, String description,
                                    @Nullable Operation.IdempotencyKey idempotencyKey,
                                    @Nullable Any meta, Instant modifiedAt, boolean done,
                                    @Nullable Any response, @Nullable Status error)
    {
        public static OperationSnapshot of(Operation original) {
            return new OperationSnapshot(original.id(), original.createdBy(), original.createdAt(),
                original.description(), original.idempotencyKey(), original.meta(), original.modifiedAt(),
                original.done(), original.response(), original.error());
        }

        public OperationSnapshot(String id, String createdBy, Instant createdAt, String description,
                                 @Nullable Operation.IdempotencyKey idempotencyKey, @Nullable Any meta,
                                 Instant modifiedAt, boolean done, @Nullable Any response,
                                 @Nullable Status error)
        {
            this.id = id;
            this.createdBy = createdBy;
            this.createdAt = createdAt;
            this.description = description;
            this.idempotencyKey = idempotencyKey;
            this.meta = meta;
            this.modifiedAt = modifiedAt;
            this.done = done;
            this.response = response;
            this.error = error;
        }

        @Override
        public String toString() {
            return "Operation{" +
                "id='" + id + '\'' +
                ", createdBy='" + createdBy + '\'' +
                ", createdAt=" + createdAt +
                ", description='" + description + '\'' +
                ", idempotencyKey='" + idempotencyKey + '\'' +
                ", meta=" + meta +
                ", modifiedAt=" + modifiedAt +
                ", done=" + done +
                ", response=" + response +
                ", error=" + error +
                '}';
        }

        public String toShortString() {
            return "Operation{id='%s', description='%s', createdBy='%s', idempotencyKey='%s', meta='%s'}"
                .formatted(id, description, createdBy, idempotencyKey, meta);
        }

        public LongRunning.Operation toProto() {
            final var builder = LongRunning.Operation.newBuilder()
                .setId(id)
                .setCreatedBy(createdBy)
                .setCreatedAt(ProtoConverter.toProto(createdAt))
                .setDescription(description)
                .setDone(done)
                .setModifiedAt(ProtoConverter.toProto(modifiedAt));
            if (meta != null) {
                builder.setMetadata(meta);
            }
            if (response != null) {
                builder.setResponse(response);
            }
            if (error != null) {
                builder.setError(ProtoConverter.toProto(error));
            }
            return builder.build();
        }
    }

    private record OperationDesc(
        Operation operation,
        @Nullable Thread thread
    ) {}
}
