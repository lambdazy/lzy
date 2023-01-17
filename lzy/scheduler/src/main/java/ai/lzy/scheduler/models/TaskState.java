package ai.lzy.scheduler.models;

import ai.lzy.model.TaskDesc;

import javax.annotation.Nullable;

public record TaskState(
    String id,
    String executionId,
    String workflowName,
    String userId,
    TaskDesc description,
    @Nullable String vmId,
    @Nullable String allocatorOperationId,
    @Nullable String workerAddress,
    @Nullable String workerPublicKey,
    @Nullable String workerOperationId
) {
    public enum Status {
        CREATED,  // Task is created, but not executing now
        ALLOCATING,  // Worker is allocating for task
        EXECUTING,  // Task executing in worker
        SUCCESS,  // Task execution completed
        ERROR  // Task execution failed
    }

    public TaskStateBuilder copy() {
        return new TaskStateBuilder(this);
    }

    public static class TaskStateBuilder {
        private final String id;
        private final String executionId;
        private final String workflowName;
        private final String userId;
        private final TaskDesc description;
        private @Nullable String vmId;
        private @Nullable String allocatorOperationId;
        private @Nullable String workerAddress;
        private @Nullable String workerPublicKey;
        private @Nullable String workerOperationId;


        public TaskStateBuilder(TaskState prev) {
            this.id = prev.id;
            this.executionId = prev.executionId;
            this.workflowName = prev.workflowName;
            this.userId = prev.userId;
            this.description = prev.description;
            this.vmId = prev.vmId;
            this.allocatorOperationId = prev.allocatorOperationId;
            this.workerAddress = prev.workerAddress;
            this.workerOperationId = prev.workerOperationId;
        }

        public TaskStateBuilder vmId(@Nullable String vmId) {
            this.vmId = vmId;
            return this;
        }

        public TaskStateBuilder allocatorOperationId(@Nullable String allocatorOperationId) {
            this.allocatorOperationId = allocatorOperationId;
            return this;
        }

        public TaskStateBuilder workerAddress(@Nullable String workerAddress) {
            this.workerAddress = workerAddress;
            return this;
        }

        public TaskStateBuilder workerOperationId(@Nullable String workerOperationId) {
            this.workerOperationId = workerOperationId;
            return this;
        }

        public TaskStateBuilder workerPublicKey(@Nullable String workerPublicKey) {
            this.workerPublicKey = workerPublicKey;
            return this;
        }

        public TaskState build() {
            return new TaskState(
                id,
                executionId,
                workflowName,
                userId,
                description,
                vmId,
                allocatorOperationId,
                workerAddress,
                workerPublicKey,
                workerOperationId
            );
        }
    }
}
