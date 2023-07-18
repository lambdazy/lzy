package ai.lzy.scheduler.models;

import ai.lzy.v1.common.LMO;
import jakarta.annotation.Nullable;

public record TaskState(
    String id,
    String executionId,
    String workflowName,
    String userId,
    String allocatorSessionId,
    LMO.TaskDesc description,
    @Nullable String vmId,
    @Nullable String allocatorOperationId,
    @Nullable Boolean vmFromCache,
    @Nullable Integer workerPort,
    @Nullable String workerHost,
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
        private final String allocatorSessionId;
        private final LMO.TaskDesc description;
        @Nullable
        private String vmId;
        @Nullable
        private String allocatorOperationId;
        @Nullable
        private Boolean vmFromCache;
        @Nullable
        private String workerHost;
        @Nullable
        private String workerOperationId;
        @Nullable
        private Integer workerPort;


        public TaskStateBuilder(TaskState prev) {
            this.id = prev.id;
            this.executionId = prev.executionId;
            this.workflowName = prev.workflowName;
            this.userId = prev.userId;
            this.allocatorSessionId = prev.allocatorSessionId;
            this.description = prev.description;
            this.vmId = prev.vmId;
            this.allocatorOperationId = prev.allocatorOperationId;
            this.vmFromCache = prev.vmFromCache;
            this.workerHost = prev.workerHost;
            this.workerOperationId = prev.workerOperationId;
            this.workerPort = prev.workerPort;
        }

        public TaskStateBuilder vmId(@Nullable String vmId) {
            this.vmId = vmId;
            return this;
        }

        public TaskStateBuilder allocatorOperationId(@Nullable String allocatorOperationId) {
            this.allocatorOperationId = allocatorOperationId;
            return this;
        }

        public TaskStateBuilder fromCache(boolean fromCache) {
            this.vmFromCache = fromCache;
            return this;
        }

        public TaskStateBuilder workerHost(@Nullable String workerAddress) {
            this.workerHost = workerAddress;
            return this;
        }

        public TaskStateBuilder workerOperationId(@Nullable String workerOperationId) {
            this.workerOperationId = workerOperationId;
            return this;
        }

        public TaskStateBuilder workerPort(@Nullable Integer workerPort) {
            this.workerPort = workerPort;
            return this;
        }

        public TaskState build() {
            return new TaskState(
                id,
                executionId,
                workflowName,
                userId,
                allocatorSessionId,
                description,
                vmId,
                allocatorOperationId,
                vmFromCache,
                workerPort,
                workerHost,
                workerOperationId
            );
        }
    }
}
