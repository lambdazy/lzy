package ai.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

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
    TaskSlotDescription taskSlotDescription,
    List<String> tasksDependedOn,
    List<String> tasksDependedFrom
) {
    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED
    }
}
