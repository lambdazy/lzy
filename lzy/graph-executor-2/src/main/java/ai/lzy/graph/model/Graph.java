package ai.lzy.graph.model;

import ai.lzy.graph.GraphExecutorApi2;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Graph(
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

    public GraphExecutorApi2.GraphExecuteResponse toGrpc() {
        return null;
    }

    public String getDescription() {
        return null;
    }
}

