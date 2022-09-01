package ai.lzy.allocator.model;

import ai.lzy.util.grpc.ProtoConverter;
import ai.lzy.v1.OperationService;
import com.google.protobuf.Any;
import io.grpc.Status;

import java.util.UUID;
import javax.annotation.Nullable;
import java.time.Instant;

public class Operation {
    private final String id;
    private final String createdBy;
    private final Instant createdAt;
    private final String description;

    private Any meta;
    private Instant modifiedAt;
    private boolean done;

    @Nullable private Any response;
    @Nullable private Status error;

    public Operation(String owner, String description, Any meta) {
        final var now = Instant.now(); // TODO: not idempotent...
        id = UUID.randomUUID().toString();
        this.meta = meta;
        this.createdBy = owner;
        this.createdAt = now;
        this.modifiedAt = now;
        this.description = description;
        done = false;
    }

    public Operation(String id, Any meta, String createdBy, Instant createdAt, Instant modifiedAt, String description,
                     boolean done, Any response, Status error)
    {
        this.id = id;
        this.meta = meta;
        this.createdBy = createdBy;
        this.createdAt = createdAt;
        this.modifiedAt = modifiedAt;
        this.description = description;
        this.done = done;
        this.response = response;
        this.error = error;
    }

    public void complete(Any response) {
        modifiedAt = Instant.now();
        done = true;
        this.response = response;
    }

    public void complete(Status error) {
        modifiedAt = Instant.now();
        done = true;
        this.error = error;
    }

    public void modifyMeta(Any meta) {
        modifiedAt = Instant.now();
        this.meta = meta;
    }

    public OperationService.Operation toProto() {
        final var builder =  OperationService.Operation.newBuilder()
            .setDescription(description)
            .setCreatedAt(ProtoConverter.toProto(createdAt))
            .setCreatedBy(createdBy)
            .setId(id)
            .setDone(done)
            .setMetadata(meta)
            .setModifiedAt(ProtoConverter.toProto(modifiedAt));
        if (response != null) {
            builder.setResponse(response);
        }
        if (error != null) {
            builder.setError(
                com.google.rpc.Status.newBuilder()
                    .setCode(error.getCode().value())
                    .setMessage(error.toString())
                    .build()
            );
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return "Operation{" +
            "id='" + id + '\'' +
            ", meta=" + meta +
            ", createdBy='" + createdBy + '\'' +
            ", createdAt=" + createdAt +
            ", modifiedAt=" + modifiedAt +
            ", description='" + description + '\'' +
            ", done=" + done +
            ", response=" + response +
            ", error=" + error +
            '}';
    }

    public String toShortString() {
        return "Operation{id='%s', description='%s', createdBy='%s', meta='%s'}"
            .formatted(id, description, createdBy, meta);
    }

    public String id() {
        return id;
    }

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

    public boolean done() {
        return done;
    }

    public Any response() {
        return response;
    }

    public Status error() {
        return error;
    }
}
