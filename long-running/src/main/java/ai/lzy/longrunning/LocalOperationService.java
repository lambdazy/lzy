package ai.lzy.longrunning;

import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;
import javax.annotation.Nullable;

public class LocalOperationService extends LongRunningServiceGrpc.LongRunningServiceImplBase {
    private static final Logger LOG = LogManager.getLogger(LocalOperationService.class);

    private final String name;

    private final Map<String, Operation> operations = new ConcurrentHashMap<>();
    private final Map<String, Operation> idempotencyKey2Operation = new ConcurrentHashMap<>();

    public LocalOperationService(String name) {
        this.name = name;
    }

    /**
     * Atomic upsert operation. Returns immutable copy added or already presented operation,
     * which should be considered as operation's state snapshot at some instant of time.
     */
    public ImmutableCopyOperation registerOperation(Operation operation) {
        LOG.info("[{}] Attempt to register operation {}", name, operation.toShortString());

        Operation.IdempotencyKey idempotencyKey = operation.idempotencyKey();

        if (idempotencyKey != null) {
            var currentAssocOp = idempotencyKey2Operation.computeIfAbsent(idempotencyKey.token(), ik -> {
                operations.putIfAbsent(operation.id(), operation);
                return operation;
            });

            if (!currentAssocOp.id().equals(operation.id())) {
                LOG.info("[{}] Found operation by idempotency key: { opId: {}, key: {} }", name, currentAssocOp.id(),
                    idempotencyKey.token());
            }

            // race condition can occur here if some other thread would change operation state right here,
            // but current clients do not behave themselves such way
            return ImmutableCopyOperation.of(currentAssocOp);
        }

        LOG.info("[{}] Register new operation {}", name, operation.toShortString());
        var op = operations.computeIfAbsent(operation.id(), id -> operation);

        if (!op.id().equals(operation.id())) {
            LOG.warn("[{}] Operation id {} already exists.", name, operation.id());
        }

        return ImmutableCopyOperation.of(op);
    }

    /**
     * Atomic get operation's state snapshot by idempotency key.
     */
    @Nullable
    public ImmutableCopyOperation getByIdempotencyKey(String key) {
        var op = idempotencyKey2Operation.get(key);
        if (op != null) {
            synchronized (op.id()) {
                return ImmutableCopyOperation.of(op);
            }
        }
        LOG.warn("Operation with idempotency key not found: { key: {} }", key);
        return null;
    }

    /**
     * Atomic updates operation and returns updated operation's state snapshot.
     */
    @Nullable
    public ImmutableCopyOperation updateResponse(String opId, Message response) {
        var op = operations.get(opId);
        if (op != null) {
            LOG.info("OpSrv-{}::update operation: { opId: {} }.", name, opId);

            synchronized (op.id()) {
                op.setResponse(response);
                return ImmutableCopyOperation.of(op);
            }
        }
        LOG.error("Operation with id not found: { opId: {} }", opId);
        return null;
    }

    /**
     * Atomic updates operation and returns updated operation's state snapshot.
     */
    @Nullable
    public ImmutableCopyOperation updateResponse(String opId, ArrayList<? extends Message> iterable) {
        var bytes = SerializationUtils.serialize(iterable);
        var bytesMessage = BytesValue.of(ByteString.copyFrom(bytes));

        return updateResponse(opId, bytesMessage);
    }

    /**
     * Atomic updates operation and returns updated operation's state snapshot.
     */
    @Nullable
    public ImmutableCopyOperation updateError(String opId, Status error) {
        var op = operations.get(opId);
        if (op != null) {
            LOG.info("OpSrv-{}::update operation: { opId: {} }.", name, opId);

            synchronized (op.id()) {
                op.setError(error);
                return ImmutableCopyOperation.of(op);
            }
        }
        LOG.error("Operation with id not found: { opId: {} }", opId);
        return null;
    }

    @Nullable
    public Boolean isDone(String operationId) {
        var op = operations.get(operationId);
        return op != null ? op.done() : null;
    }

    @Override
    public void get(LongRunning.GetOperationRequest request, StreamObserver<LongRunning.Operation> response) {
        LOG.info("OpSrv-{}::get op {}.", name, request.getOperationId());

        var op = operations.get(request.getOperationId());
        if (op == null) {
            var msg = "Operation %s not found".formatted(request.getOperationId());
            LOG.error("OpSrv-%s::get error: %s".formatted(name, msg));
            response.onError(Status.NOT_FOUND.withDescription(msg).asException());
            return;
        }

        LongRunning.Operation protoOp;

        synchronized (op.id()) {
            if (op.done()) {
                if (op.response() != null) {
                    LOG.info("OpSrv-{}::get: operation {} successfully completed.", name, op.id());
                } else if (op.error() != null) {
                    LOG.info("OpSrv-{}::get: operation {} failed with error {}.", name, op.id(), op.error());
                } else {
                    LOG.error("OpSrv-{}::get: operation {} is in unknown completed state {}.",
                        name, op.id(), op.toString());
                }
            } else {
                LOG.info("OpSrv{}::get: operation {} is in progress", name, op.id());
            }
            protoOp = op.toProto();
        }

        response.onNext(protoOp);
        response.onCompleted();
    }

    @Nullable
    public ImmutableCopyOperation awaitOperationCompletion(String opId, Duration loadAttemptDelay, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        ImmutableCopyOperation opSnapshot = null;

        while (true) {
            var op = operations.get(opId);
            if (op != null) {
                synchronized (op.id()) {
                    opSnapshot = ImmutableCopyOperation.of(op);
                }
            }

            if (opSnapshot == null || opSnapshot.done() || deadline - System.nanoTime() <= 0L) {
                break;
            }

            LockSupport.parkNanos(loadAttemptDelay.toNanos());
        }

        return opSnapshot;
    }

    public record ImmutableCopyOperation(String id, String createdBy, Instant createdAt, String description,
                                         @Nullable Operation.IdempotencyKey idempotencyKey,
                                         @Nullable Any meta, Instant modifiedAt, boolean done,
                                         @Nullable Any response, @Nullable Status error)
    {
        public static ImmutableCopyOperation of(Operation original) {
            return new ImmutableCopyOperation(original.id(), original.createdBy(), original.createdAt(),
                original.description(), original.idempotencyKey(), original.meta(), original.modifiedAt(),
                original.done(), original.response(), original.error());
        }

        public ImmutableCopyOperation(String id, String createdBy, Instant createdAt, String description,
                                      @Nullable Operation.IdempotencyKey idempotencyKey, @Nullable Any meta,
                                      Instant modifiedAt, boolean done, @Nullable Any response, @Nullable Status error)
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
                builder.setError(
                    com.google.rpc.Status.newBuilder()
                        .setCode(error.getCode().value())
                        .setMessage(error.toString())
                        .build());
            }
            return builder.build();
        }
    }
}
