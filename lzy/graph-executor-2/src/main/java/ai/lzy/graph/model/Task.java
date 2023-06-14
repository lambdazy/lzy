package ai.lzy.graph.model;

import ai.lzy.v1.common.LMO;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Task(
    String id,
    String name,
    String graphId,
    Status status,
    String workflowId,
    String workflowName,
    String userId,
    String errorDescription,
    LMO.Operation operation,
    Map<String, String> slotsToChannelsAssignments,
    List<String> tasksDependedOn,
    List<String> tasksDependedFrom
) {
    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED
    }

    @Nullable
    public String getDescription() {
        return null;
    }
}
