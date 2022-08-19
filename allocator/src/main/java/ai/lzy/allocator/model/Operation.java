package ai.lzy.allocator.model;

import ai.lzy.v1.OperationService;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import io.grpc.Status;

import javax.annotation.Nullable;
import java.time.Instant;

public record Operation(
    String id,
    Any meta,
    String createdBy,
    Instant createdAt,
    Instant modifiedAt,
    String description,
    boolean done,

    @Nullable Any response,
    @Nullable Status error
) {

    public Operation complete(Any response) {
        return new Operation(id, meta, createdBy, createdAt, Instant.now(), description, true, response, null);
    }

    public Operation complete(Status error) {
        return new Operation(id, meta, createdBy, createdAt, Instant.now(), description, true, null, error);
    }

    public Operation modifyMeta(Any meta) {
        return new Operation(id, meta, createdBy, createdAt, Instant.now(), description, done, response, error);
    }

    public OperationService.Operation toGrpc() {
        final var builder =  OperationService.Operation.newBuilder()
            .setDescription(this.description())
            .setCreatedAt(Timestamp.newBuilder()
                .setSeconds(createdAt.getEpochSecond())
                .setNanos(createdAt.getNano())
                .build())
            .setCreatedBy(this.createdBy())
            .setId(this.id())
            .setDone(this.done())
            .setMetadata(this.meta())
                .setModifiedAt(
                    Timestamp.newBuilder()
                        .setSeconds(modifiedAt.getEpochSecond())
                        .setNanos(modifiedAt.getNano())
                        .build());
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
}
