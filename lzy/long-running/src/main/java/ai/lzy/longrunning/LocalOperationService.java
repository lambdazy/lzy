package ai.lzy.longrunning;

import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public class LocalOperationService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LocalOperationService.class);

    private final String name;

    private final Map<String, Operation> operations = new ConcurrentHashMap<>();
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
        LOG.debug("[{}] Attempt to register operation {}", name, operation.toShortString());

        Operation.IdempotencyKey idempotencyKey = operation.idempotencyKey();

        if (idempotencyKey != null) {
            var currentAssocOp = idempotencyKey2Operation.computeIfAbsent(idempotencyKey.token(), ik -> {
                operations.putIfAbsent(operation.id(), operation);
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
        var op = operations.computeIfAbsent(operation.id(), id -> operation);

        if (!op.id().equals(operation.id())) {
            LOG.warn("[{}] Operation with id {} already exists.", name, operation.id());
        }

        return OperationSnapshot.of(op);
    }

    /**
     * Atomic get operation's state snapshot by idempotency key.
     */
    @Nullable
    public OperationSnapshot getByIdempotencyKey(String key) {
        var op = idempotencyKey2Operation.get(key);
        if (op != null) {
            LOG.debug("[{}] Got operation by idempotency key: { key: {}, opId: {} }", name,
                key, op.id());
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

            synchronized (op.id()) {
                op.setResponse(response);
                op.id().notifyAll();
                return OperationSnapshot.of(op);
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

            synchronized (op.id()) {
                op.setError(error);
                op.id().notifyAll();
                return OperationSnapshot.of(op);
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

            synchronized (op.id()) {
                return OperationSnapshot.of(op);
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

        synchronized (op.id()) {
            if (op.done()) {
                if (op.response() != null) {
                    LOG.info("[{}] Got operation is successfully completed: { opId: {} }", name, op.id());
                } else if (op.error() != null) {
                    LOG.info("[{}] Got operation is failed: { opId: {}, error: {} }", name, op.id(), op.error());
                } else {
                    LOG.error("[{}] Got completed operation is in unknown state: { opId: {}, state: {} }", name,
                        op.id(), op.toString());
                }
            } else {
                LOG.info("[{}] Got operation is in progress: { opId: {} }", name, op.id());
            }
            protoOp = op.toProto();
        }

        response.onNext(protoOp);
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
            synchronized (op.id()) {
                while (!op.done() && (nanos = deadline - System.nanoTime()) > 0L) {
                    op.id().wait(Duration.ofNanos(nanos).toMillis());
                }
                waited = op.done();
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
}
