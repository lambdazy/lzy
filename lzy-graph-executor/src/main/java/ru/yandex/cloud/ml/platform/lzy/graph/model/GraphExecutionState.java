package ru.yandex.cloud.ml.platform.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.GraphExecutionStatus.Completed;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.GraphExecutionStatus.Executing;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.GraphExecutionStatus.Failed;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.GraphExecutionStatus.Waiting;

import java.util.ArrayList;
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

    public GraphExecutionState(
        String workflowId,
        String id,
        GraphDescription description
    ) {
        this(workflowId, id, description, new ArrayList<>(), new ArrayList<>(), Status.WAITING, null);
    }

    public GraphExecutionState(
        String workflowId,
        String id,
        GraphDescription description,
        List<TaskExecution> executions,
        List<TaskExecution> currentExecutionGroup,
        Status status
    ) {
        this(workflowId, id, description, executions, currentExecutionGroup, status, null);
    }

    public enum Status {
       WAITING, EXECUTING, COMPLETED, SCHEDULED_TO_FAIL, FAILED
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
            case EXECUTING, SCHEDULED_TO_FAIL -> statusBuilder.setExecuting(
                Executing.newBuilder().build() //TODO(artolord) add tasks progress here
            );
            default -> { } // Unreachable
        }
        return statusBuilder.build();
    }
}
