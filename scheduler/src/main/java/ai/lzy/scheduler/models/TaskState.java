package ai.lzy.scheduler.models;

import ai.lzy.model.TaskDesc;

import javax.annotation.Nullable;

public record TaskState(
    String id,
    String workflowId,
    String workflowName,
    String userId,
    TaskDesc description,
    Status status,

    @Nullable Integer returnCode,
    @Nullable String errorDescription,
    @Nullable String servantId
) {
    public enum Status {
        QUEUE,  // Task is in scheduler queue
        SCHEDULED,  // Task is scheduled to servant, but event not processed yet
        EXECUTING,  // Task executing in servant
        SUCCESS,  // Task execution completed
        ERROR  // Task execution failed
    }
}
