package ai.lzy.scheduler.models;

import ai.lzy.model.operation.Operation;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;

import javax.annotation.Nullable;

public record ServantState(
    String id,
    String workflowName,
    Operation.Requirements requirements,
    Status status,

    @Nullable String errorDescription,
    @Nullable String taskId,
    @Nullable HostAndPort servantUrl
) {

    public enum Status {
        CREATED,  // Servant created, but allocation not started
        CONNECTING,  // Allocation requested, but servant not connected
        CONFIGURING,  // Installing env on servant
        IDLE,  // Servant env configured and ready to execute tasks
        RUNNING, // Servant ready to execute tasks, but it contains data from other tasks
        EXECUTING,  // Servant executing task
        STOPPING,  // Servant gracefully stopping
        DESTROYED  // Servant destroyed and cannot be used anymore
    }

    public ServantStateBuilder copy() {
        return new ServantStateBuilder(id, workflowName, requirements, status)
            .setTaskId(taskId)
            .setErrorDescription(errorDescription)
            .setServantUrl(servantUrl);
    }

    @Override
    public String toString() {
        return String.format("<workflowName: %s, id: %s, status: %s>", workflowName, id, status);
    }

    public static class ServantStateBuilder {
        private final String id;
        private final String workflowName;
        private final Operation.Requirements requirements;
        private Status status;

        @Nullable private String taskId = null;
        @Nullable private String errorDescription = null;
        @Nullable private HostAndPort servantUrl = null;

        public ServantStateBuilder(String id, String workflowName, Operation.Requirements requirements, Status status) {
            this.id = id;
            this.workflowName = workflowName;
            this.requirements = requirements;
            this.status = status;
        }

        public ServantStateBuilder setStatus(Status status) {
            this.status = status;
            return this;
        }

        public ServantStateBuilder setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public ServantStateBuilder setErrorDescription(String errorDescription) {
            this.errorDescription = errorDescription;
            return this;
        }

        public ServantStateBuilder setServantUrl(HostAndPort servantUrl) {
            this.servantUrl = servantUrl;
            return this;
        }

        public ServantState build() {
            return new ServantState(id, workflowName, requirements, status, errorDescription,
                taskId, servantUrl);
        }
    }
}
