package ai.lzy.longrunning;

import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;

public class Operation {

    public record IdempotencyKey(
        String token,
        String requestHash
    ) {}

    private final String id;
    private final String createdBy;
    private final Instant createdAt;
    private final String description;
    @Nullable
    private final Instant deadline;
    @Nullable
    private final IdempotencyKey idempotencyKey;

    @Nullable
    private Any meta;
    private Instant modifiedAt;
    private boolean done;

    @Nullable
    private Any response;
    @Nullable
    private Status error;

    public static Operation createCompleted(String id, String createdBy, String description,
                                            @Nullable IdempotencyKey idempotencyKey, @Nullable Message meta,
                                            Message response)
    {
        Objects.requireNonNull(response);
        var now = Instant.now();
        return new Operation(id, createdBy, now, description, null, idempotencyKey,
            meta != null ? Any.pack(meta) : null, now, true, Any.pack(response), null);
    }

    public static Operation create(String createdBy, String description, @Nullable Instant deadline,
                                   @Nullable IdempotencyKey idempotencyKey, @Nullable Message meta)
    {
        var now = Instant.now();
        return new Operation(UUID.randomUUID().toString(), createdBy, now, description, deadline, idempotencyKey,
            meta != null ? Any.pack(meta) : null, now, false, null, null);
    }

    public static Operation create(String createdBy, String description, @Nullable Instant deadline,
                                   @Nullable Message meta)
    {
        return create(createdBy, description, deadline, null, meta);
    }

    public Operation(String id, String createdBy, Instant createdAt, String description, Instant deadline,
                     @Nullable IdempotencyKey idempotencyKey, @Nullable Any meta, Instant modifiedAt,
                     boolean done, @Nullable Any response, @Nullable Status error)
    {
        this.id = id;
        this.createdBy = createdBy;
        this.createdAt = createdAt;
        this.description = description;
        this.deadline = deadline;
        this.idempotencyKey = idempotencyKey;
        this.meta = meta;
        this.modifiedAt = modifiedAt;
        this.done = done;
        this.response = response;
        this.error = error;
    }

    public void setResponse(Any response) {
        modifiedAt = Instant.now();
        done = true;
        this.response = response;
    }

    public void setResponse(Message response) {
        setResponse(Any.pack(response));
    }

    public void setError(Status error) {
        modifiedAt = Instant.now();
        done = true;
        this.error = error;
    }

    public void modifyMeta(Any meta) {
        modifiedAt = Instant.now();
        this.meta = meta;
    }

    public void modifyMeta(Message meta) {
        modifyMeta(Any.pack(meta));
    }

    public LongRunning.Operation toProto() {
        final var builder = LongRunning.Operation.newBuilder()
            .setId(id)
            .setCreatedBy(createdBy)
            .setCreatedAt(toProto(createdAt))
            .setDescription(description)
            .setDone(done)
            .setModifiedAt(toProto(modifiedAt));
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

    @Override
    public String toString() {
        return "Operation{" +
            "id='" + id + '\'' +
            ", createdBy='" + createdBy + '\'' +
            ", createdAt=" + createdAt +
            ", description='" + description + '\'' +
            ", deadline='" + deadline + '\'' +
            ", idempotencyKey='" + idempotencyKey + '\'' +
            ", meta=" + meta +
            ", modifiedAt=" + modifiedAt +
            ", done=" + done +
            ", response=" + response +
            ", error=" + error +
            '}';
    }

    public String toShortString() {
        return "Operation{id='%s', description='%s', createdBy='%s', deadline='%s', idempotencyKey='%s', meta='%s'}"
            .formatted(id, description, createdBy, deadline, idempotencyKey, meta);
    }

    public String id() {
        return id;
    }

    @Nullable
    public Any meta() {
        return meta;
    }

    public String createdBy() {
        return createdBy;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant modifiedAt() {
        return modifiedAt;
    }

    public String description() {
        return description;
    }

    @Nullable
    public Instant deadline() {
        return deadline;
    }

    @Nullable
    public IdempotencyKey idempotencyKey() {
        return idempotencyKey;
    }

    public boolean done() {
        return done;
    }

    @Nullable
    public Any response() {
        return response;
    }

    @Nullable
    public Status error() {
        return error;
    }

    @SuppressWarnings("CheckStyle")
    private static Timestamp toProto(Instant instant) {
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }
}
