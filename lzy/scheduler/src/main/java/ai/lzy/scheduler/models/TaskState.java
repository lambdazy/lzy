package ai.lzy.scheduler.models;

import ai.lzy.model.TaskDesc;

import javax.annotation.Nullable;

public record TaskState(
    String id,
    String executionId,
    String workflowName,
    String userId,
    TaskDesc description,
    Status status,

    @Nullable Integer returnCode,
    @Nullable String errorDescription,
    @Nullable String vmId,
    @Nullable String allocatorOperationId,
    @Nullable String workerAddress,
    @Nullable String workerOperationId
) {
    public enum Status {
        CREATED,  // Task is created, but not executing now
        ALLOCATING,  // Worker is allocating for task
        EXECUTING,  // Task executing in worker
        SUCCESS,  // Task execution completed
        ERROR  // Task execution failed
    }

    public static class TaskStateBuilder {
        private final String id;
        private final String executionId;
        private final String workflowName;
        private final String userId;
        private final TaskDesc description;
        private final Status status;

        private @Nullable Integer returnCode;
        private @Nullable String errorDescription;
        private @Nullable String vmId;
        private @Nullable String allocatorOperationId;
        private @Nullable String workerAddress;
        private @Nullable String workerOperationId;


        public TaskStateBuilder(TaskState prev) {
            this.id = prev.id;
            this.executionId = prev.executionId;
            this.workflowName = prev.workflowName;
            this.userId = prev.userId;
            this.description = prev.description;
            this.status = prev.status;
        }
    }
}
