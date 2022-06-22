package ai.lzy.scheduler.models;

import java.time.LocalDateTime;
import javax.annotation.Nullable;

public record TaskEvent(
    String id,
    LocalDateTime timestamp,

    String workflowId,
    String taskId,
    Type type,
    @Nullable String description,
    @Nullable String rc
) {

    public enum Type {
        QUEUED,
        SCHEDULED,
        COMPLETED,
        FAILED
    }
}
