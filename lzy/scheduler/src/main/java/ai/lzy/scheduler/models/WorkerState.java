package ai.lzy.scheduler.models;

import ai.lzy.model.operation.Operation;
import com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public record WorkerState(
    String id,
    String userId,
    String workflowName,
    Operation.Requirements requirements,
    Status status,

    @Nullable String errorDescription,
    @Nullable String taskId,
    @Nullable HostAndPort workerUrl
) {

    public enum Status {
        CREATED,  // Worker created, but allocation not started
        CONNECTING,  // Allocation requested, but worker not connected
        CONFIGURING,  // Installing env on worker
        IDLE,  // Worker env configured and ready to execute tasks
        RUNNING, // Worker ready to execute tasks, but it contains data from other tasks
        EXECUTING,  // Worker executing task
        STOPPING,  // Worker gracefully stopping
        DESTROYED  // Worker destroyed and cannot be used anymore
    }

    public WorkerStateBuilder copy() {
        return new WorkerStateBuilder(id, userId, workflowName, requirements, status)
            .setTaskId(taskId)
            .setErrorDescription(errorDescription)
            .setWorkerUrl(workerUrl);
    }

    @Override
    public String toString() {
        return String.format("<userId: %s, workflowName: %s, id: %s, status: %s>", userId, workflowName, id, status);
    }

    public static class WorkerStateBuilder {
        private final String id;
        private final String userId;
        private final String workflowName;
        private final Operation.Requirements requirements;
        private Status status;

        @Nullable private String taskId = null;
        @Nullable private String errorDescription = null;
        @Nullable private HostAndPort workerUrl = null;

        public WorkerStateBuilder(String id, String userId, String workflowName,
                                  Operation.Requirements requirements, Status status)
        {
            this.id = id;
            this.userId = userId;
            this.workflowName = workflowName;
            this.requirements = requirements;
            this.status = status;
        }

        public WorkerStateBuilder setStatus(Status status) {
            this.status = status;
            return this;
        }

        public WorkerStateBuilder setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public WorkerStateBuilder setErrorDescription(String errorDescription) {
            this.errorDescription = errorDescription;
            return this;
        }

        public WorkerStateBuilder setWorkerUrl(HostAndPort workerUrl) {
            this.workerUrl = workerUrl;
            return this;
        }

        public WorkerState build() {
            return new WorkerState(id, userId, workflowName, requirements, status, errorDescription,
                taskId, workerUrl);
        }
    }
}
