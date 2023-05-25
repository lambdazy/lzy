package ai.lzy.graph.model;

import java.util.List;

import ai.lzy.graph.GraphExecutorApi2;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Graph(
    String id,
    Status status,
    String workflowId,
    String workflowName,
    String userId,
    List<String> waitingTasks,
    List<String> executingTasks,
    List<String> completedTasks,
    List<String> failedTasks
) {
    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED
    }

    public GraphExecutorApi2.GraphExecuteResponse toGrpc() {
        return null;
    }
}

