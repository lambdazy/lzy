package ai.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;

import java.time.Instant;
import java.util.Objects;

@Builder(toBuilder = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskOperation(
    String id,
    String taskId,
    Instant startedAt,
    Status status,
    String errorDescription,
    State state
) {
    public enum Status {
        WAITING, ALLOCATING, EXECUTING, COMPLETED, FAILED
    }

    @Builder(toBuilder = true)
    public record State(
        String allocOperationId,
        String vmId,
        boolean fromCache,
        String workerHost,
        int workerPort,
        String workerOperationId
    ) {}

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskOperation that = (TaskOperation) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
