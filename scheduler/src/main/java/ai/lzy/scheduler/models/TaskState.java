package ai.lzy.scheduler.models;

import javax.annotation.Nullable;

public record TaskState(
    String id,
    String workflowId,
    TaskDesc description,
    Status status,

    @Nullable Integer returnCode,
    @Nullable String errorDescription,
    @Nullable String servantId
) {
    public enum Status {
        QUEUE,
        EXECUTING,
        SUCCESS,
        ERROR
    }
}
