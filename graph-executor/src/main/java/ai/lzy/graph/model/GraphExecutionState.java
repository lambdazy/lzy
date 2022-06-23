package ai.lzy.graph.model;

import ai.lzy.priv.v2.graph.GraphExecutorApi;
import ai.lzy.priv.v2.graph.GraphExecutorApi.GraphExecutionStatus.Completed;
import ai.lzy.priv.v2.graph.GraphExecutorApi.GraphExecutionStatus.Executing;
import ai.lzy.priv.v2.graph.GraphExecutorApi.GraphExecutionStatus.Failed;
import ai.lzy.priv.v2.graph.GraphExecutorApi.GraphExecutionStatus.Waiting;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphExecutionState(
        String workflowId,
        String id,
        GraphDescription description,
        List<TaskExecution> executions,
        List<TaskExecution> currentExecutionGroup,
        Status status,
        String errorDescription
) {

    public enum Status {
       WAITING, EXECUTING, COMPLETED, FAILED
    }

    public GraphExecutorApi.GraphExecutionStatus toGrpc() {
        GraphExecutorApi.GraphExecutionStatus.Builder statusBuilder = GraphExecutorApi.GraphExecutionStatus.newBuilder()
            .setWorkflowId(workflowId)
            .setGraphId(id);
        switch (status) {
            case WAITING -> statusBuilder.setWaiting(Waiting.newBuilder().build());
            case COMPLETED -> statusBuilder.setCompleted(Completed.newBuilder().build());
            case FAILED -> statusBuilder.setFailed(
                Failed.newBuilder()
                    .setDescription(errorDescription)
                    .build()
            );
            case EXECUTING -> statusBuilder.setExecuting(
                Executing.newBuilder().build() //TODO(artolord) add tasks progress here
            );
            default -> { } // Unreachable
        }
        return statusBuilder.build();
    }

    @Override
    public String toString() {
        String tasks = Arrays.toString(
            description.tasks()
                .stream()
                .map(TaskDescription::id)
                .toArray()
        );
        String execGroup = Arrays.toString(
            currentExecutionGroup
                .stream()
                .map(TaskExecution::description)
                .map(TaskDescription::id)
                .toArray()
        );
        return String.format("""
                GraphExecutionState{
                    workflowId: %s,
                    id: %s,
                    tasks: %s,
                    currentExecutionGroup: %s,
                    status: %s,
                    errorDescription: %s
                """, workflowId, id, tasks, execGroup, status, errorDescription
            );
    }

    public static GraphExecutionStateBuilder builder() {
        return new GraphExecutionStateBuilder();
    }

    public GraphExecutionStateBuilder copyFromThis() {
        return new GraphExecutionStateBuilder()
            .withWorkflowId(workflowId)
            .withId(id)
            .withDescription(description)
            .withExecutions(executions)
            .withErrorDescription(errorDescription)
            .withCurrentExecutionGroup(currentExecutionGroup)
            .withStatus(status);
    }

    public static class GraphExecutionStateBuilder {
        private String workflowId = null;
        private String id = null;
        private GraphDescription description = null;
        private List<TaskExecution> executions = new ArrayList<>();
        private List<TaskExecution> currentExecutionGroup = new ArrayList<>();
        private Status status = Status.WAITING;
        private String errorDescription = null;

        public GraphExecutionStateBuilder withWorkflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public GraphExecutionStateBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public GraphExecutionStateBuilder withDescription(GraphDescription description) {
            this.description = description;
            return this;
        }

        public GraphExecutionStateBuilder withExecutions(List<TaskExecution> executions) {
            this.executions = executions;
            return this;
        }

        public GraphExecutionStateBuilder withStatus(Status status) {
            this.status = status;
            return this;
        }

        public GraphExecutionStateBuilder withCurrentExecutionGroup(List<TaskExecution> currentExecutionGroup) {
            this.currentExecutionGroup = currentExecutionGroup;
            return this;
        }

        public GraphExecutionStateBuilder withErrorDescription(String errorDescription) {
            this.errorDescription = errorDescription;
            return this;
        }

        public GraphExecutionState build() {
            if (workflowId == null || id == null || description == null) {
                throw new NullPointerException(
                    "Cannot build GraphExecutionState with workflowId, id or description == null");
            }
            return new GraphExecutionState(
                workflowId, id, description, executions, currentExecutionGroup,
                status, errorDescription
            );
        }
    }
}
