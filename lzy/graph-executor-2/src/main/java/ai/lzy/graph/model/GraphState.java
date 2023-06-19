package ai.lzy.graph.model;

import ai.lzy.graph.GraphExecutorApi2;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphState(
    String id,
    String operationId,
    Status status,
    String workflowId,
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

    public GraphExecutorApi2.GraphExecuteResponse toProto() {
        var builder = GraphExecutorApi2.GraphExecuteResponse.newBuilder()
            .setGraphId(id)
            .setWorkflowId(workflowId);
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
                    // Add info about task
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
}

