package ai.lzy.scheduler.models;

import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;

public record ServantState(
    String id,
    String workflowId,
    Provisioning provisioning,
    Status status,
    Env env,

    @Nullable String errorDescription,
    @Nullable String taskId
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
        return new ServantStateBuilder()
            .setId(id)
            .setWorkflowId(workflowId)
            .setStatus(status)
            .setEnv(env)
            .setTaskId(taskId)
            .setErrorDescription(errorDescription)
            .setProvisioning(provisioning);
    }

    public static class ServantStateBuilder {
        private String id;
        private String workflowId;
        private Provisioning provisioning;
        private Status status;

        private String taskId;
        private Env env;
        private String errorDescription;

        public ServantStateBuilder setId(String id) {
            this.id = id;
            return this;
        }

        public ServantStateBuilder setWorkflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public ServantStateBuilder setProvisioning(Provisioning provisioning) {
            this.provisioning = provisioning;
            return this;
        }

        public ServantStateBuilder setStatus(Status status) {
            this.status = status;
            return this;
        }

        public ServantStateBuilder setTaskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public ServantStateBuilder setEnv(Env env) {
            this.env = env;
            return this;
        }

        public ServantStateBuilder setErrorDescription(String errorDescription) {
            this.errorDescription = errorDescription;
            return this;
        }

        public ServantState build() {
            return new ServantState(id, workflowId, provisioning, status, env, errorDescription, taskId);
        }
    }
}
