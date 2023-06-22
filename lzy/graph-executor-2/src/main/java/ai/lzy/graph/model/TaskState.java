package ai.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskState(
    String id,
    String name,
    String graphId,
    Status status,
    String workflowId,
    String workflowName,
    String userId,
    String errorDescription,
    TaskSlotDescription taskSlotDescription,
    List<String> tasksDependedOn, // tasks, on which this task is depended on
    List<String> tasksDependedFrom // tasks, that are depended from this task
) {
    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED
    }
}
