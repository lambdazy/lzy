package ai.lzy.graph.model;

import ai.lzy.graph.GraphExecutorApi2;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Builder
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphState(
    String id,
    String operationId,
    Status status,
    String executionId,
    String workflowName,
    String userId,
    Map<Status, List<String>> tasks,
    String errorDescription,
    String failedTaskId,
    String failedTaskName
) {
    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED
    }

    @Override
    public String toString() {
        String taskDescr = tasks.keySet().stream()
            .map(key -> "%s = {%s}".formatted(key, String.join(", ", tasks.get(key))))
            .collect(Collectors.joining("\n"));

        return String.format("""
                GraphState{
                    executionId: %s,
                    id: %s,
                    status: %s,
                    errorDescription: %s,
                    tasks: %s,
                """, executionId, id, status, errorDescription, taskDescr
            );
    }

    public GraphStateBuilder copyFromThis() {
        return new GraphStateBuilder()
            .id(id)
            .operationId(operationId)
            .status(status)
            .executionId(executionId)
            .workflowName(workflowName)
            .userId(userId)
            .tasks(tasks)
            .errorDescription(errorDescription)
            .failedTaskId(failedTaskId)
            .failedTaskName(failedTaskName);
    }

    public static GraphState fromProto(GraphExecutorApi2.GraphExecuteRequest graphDesc,
                                       String graphId, String operationId)
    {
        return new GraphState(
            graphId,
            operationId,
            Status.WAITING,
            graphDesc.getExecutionId(),
            graphDesc.getWorkflowName(),
            graphDesc.getUserId(),
            new HashMap<>(),
            null,
            null,
            null
        );
    }

    public GraphExecutorApi2.GraphExecuteResponse toProto(
        Map<String, GraphExecutorApi2.TaskExecutionStatus> taskProtos)
    {
        var builder = GraphExecutorApi2.GraphExecuteResponse.newBuilder()
            .setGraphId(id)
            .setWorkflowId(executionId);
        switch (status) {
            case WAITING -> builder.setWaiting(
                GraphExecutorApi2.GraphExecuteResponse.Waiting.newBuilder().build()
            );
            case COMPLETED -> builder.setCompleted(
                GraphExecutorApi2.GraphExecuteResponse.Completed.newBuilder().build()
            );
            case FAILED -> builder.setFailed(
                GraphExecutorApi2.GraphExecuteResponse.Failed.newBuilder()
                    .setFailedTaskId(failedTaskId)
                    .setFailedTaskName(failedTaskName)
                    .setDescription(errorDescription)
                    .build()
            );
            case EXECUTING -> {
                final List<GraphExecutorApi2.TaskExecutionStatus> statuses = new ArrayList<>();
                for (var task: tasks.get(Status.EXECUTING)) {
                    statuses.add(taskProtos.get(task));
                }
                builder.setExecuting(
                    GraphExecutorApi2.GraphExecuteResponse.Executing.newBuilder()
                        .addAllExecutingTasks(statuses)
                        .build()
                );
            }
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphState that = (GraphState) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

