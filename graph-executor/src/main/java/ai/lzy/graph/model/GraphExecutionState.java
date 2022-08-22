package ai.lzy.graph.model;

import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.v1.SchedulerApi.TaskStatus;
import ai.lzy.v1.graph.GraphExecutorApi;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecutionStatus.Completed;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecutionStatus.Executing;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecutionStatus.Failed;
import ai.lzy.v1.graph.GraphExecutorApi.GraphExecutionStatus.Waiting;
import ai.lzy.v1.graph.GraphExecutorApi.TaskExecutionStatus;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphExecutionState(
        String workflowId,
        String workflowName,
        String id,
        GraphDescription description,
        List<TaskExecution> executions,
        List<TaskExecution> currentExecutionGroup,
        Status status,
        String errorDescription
) {

    private static final Logger LOG = LogManager.getLogger(GraphExecutionState.class);

    public enum Status {
       WAITING, EXECUTING, COMPLETED, FAILED
    }

    public GraphExecutorApi.GraphExecutionStatus toGrpc(SchedulerApi schedulerApi) {
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
            case EXECUTING -> {
                final List<TaskExecutionStatus> statuses = new ArrayList<>();
                for (var task: executions) {
                    final TaskStatus status;
                    try {
                        status = schedulerApi.status(workflowId, task.id());
                    } catch (StatusRuntimeException e) {
                        LOG.error("Cannot get status of task", e);
                        statuses.add(TaskExecutionStatus.newBuilder()
                            .setTaskDescriptionId(task.description().id())
                            .build());
                        continue;
                    }
                    statuses.add(TaskExecutionStatus.newBuilder()
                        .setProgress(status)
                        .setTaskDescriptionId(task.description().id())
                        .build());
                }
                statusBuilder.setExecuting(
                    Executing.newBuilder()
                        .addAllExecutingTasks(statuses)
                        .build()
                );
            }
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
            .withWorkflowName(workflowName)
            .withId(id)
            .withDescription(description)
            .withExecutions(executions)
            .withErrorDescription(errorDescription)
            .withCurrentExecutionGroup(currentExecutionGroup)
            .withStatus(status);
    }

    public static class GraphExecutionStateBuilder {
        private String workflowId = null;
        private String workflowName = null;
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

        public GraphExecutionStateBuilder withWorkflowName(String workflowName) {
            this.workflowName = workflowName;
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
                workflowId, workflowName, id, description, executions, currentExecutionGroup,
                status, errorDescription
            );
        }
    }
}
