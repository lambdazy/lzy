package ai.lzy.graph.model;

import java.time.Instant;

public record TaskOperation(
    String id,
    String taskId,
    Instant startedAt,
    Instant deadline,
    String ownerInstanceId,

    Status status,
    String errorDescription,
    Runnable deferredAction
) {
    public enum Status {
        WAITING, ALLOCATING, EXECUTING, COMPLETED, FAILED
    }

    public TaskOperation withDeferredAction(Runnable deferredAction) {
        return new TaskOperation(id, taskId, startedAt, deadline, ownerInstanceId, status,
            errorDescription, deferredAction);
    }
}
