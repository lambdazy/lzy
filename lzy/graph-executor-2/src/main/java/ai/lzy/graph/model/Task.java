package ai.lzy.graph.model;

import ai.lzy.v1.common.LMO;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Task(
    String id,
    String graphId,
    Status status,
    String workflowId,
    String workflowName,
    String userId,
    LMO.Operation operation,
    Map<String, String> slotsToChannelsAssignments,
    List<String> tasksDependedOn,
    List<String> tasksDependedFrom
) {
    private static final Logger LOG = LogManager.getLogger(Task.class);

    public enum Status {
        WAITING, EXECUTING, COMPLETED, FAILED
    }
}
