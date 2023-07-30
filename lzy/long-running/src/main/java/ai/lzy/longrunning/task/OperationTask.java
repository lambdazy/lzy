package ai.lzy.longrunning.task;

import jakarta.annotation.Nullable;

import java.time.Instant;
import java.util.Map;

public record OperationTask(
    long id,
    String name,
    String entityId,
    String type,
    Status status,
    Instant createdAt,
    Instant updatedAt,
    Map<String, Object> metadata,
    @Nullable
    String operationId,
    @Nullable
    String workerId,
    @Nullable
    Instant leaseTill
) {
    public static OperationTask createPending(String name, String entityId, String type, Map<String, Object> metadata) {
        return new OperationTask(-1, name, entityId, type, Status.PENDING, Instant.now(), Instant.now(),
            metadata, null, null, null);
    }

    public enum Status {
        PENDING,    //just created and waiting to be executed
        RUNNING,    //acquired by one of the instance of app and executing
        FINISHED,   //successful finish
        FAILED,     //unrecoverable failure
        STALE,      //executed too late and not applicable anymore
    }

    public record Update(
        @Nullable Status status,
        @Nullable Map<String, Object> metadata,
        @Nullable String operationId
    ) {
        public boolean isEmpty() {
            return status == null && metadata == null && operationId == null;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private Status status;
            private Map<String, Object> metadata;
            private String operationId;

            public Builder status(Status status) {
                this.status = status;
                return this;
            }

            public Builder metadata(Map<String, Object> metadata) {
                this.metadata = metadata;
                return this;
            }

            public Builder operationId(String operationId) {
                this.operationId = operationId;
                return this;
            }

            public Update build() {
                return new Update(status, metadata, operationId);
            }
        }
    }
}
