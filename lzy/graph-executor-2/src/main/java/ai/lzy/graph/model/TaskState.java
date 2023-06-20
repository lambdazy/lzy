package ai.lzy.graph.model;

import ai.lzy.graph.GraphExecutorApi2;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

    public static TaskState fromProto(GraphExecutorApi2.GraphExecuteRequest.TaskDesc taskDesc, GraphState graphState) {
        List<TaskSlotDescription.Slot> slots = taskDesc.getOperation().getSlotsList().stream()
            .map(slot -> new TaskSlotDescription.Slot(
                slot.getName(),
                TaskSlotDescription.Slot.Media.valueOf(slot.getMedia().name()),
                TaskSlotDescription.Slot.Direction.valueOf(slot.getDirection().name()),
                slot.getContentType().getDataFormat(),
                slot.getContentType().getSchemeFormat(),
                slot.getContentType().getSchemeContent(),
                slot.getContentType().getMetadataMap()
            ))
            .toList();
        Map<String, String> slotAssignments = taskDesc.getSlotAssignmentsList().stream()
            .collect(Collectors.toMap(t -> t.getSlotName(), t -> t.getChannelId()));
        TaskSlotDescription taskSlotDescription = new TaskSlotDescription(
            taskDesc.getOperation().getName(),
            taskDesc.getOperation().getDescription(),
            taskDesc.getOperation().getRequirements().getPoolLabel(),
            taskDesc.getOperation().getRequirements().getZone(),
            taskDesc.getOperation().getCommand(),
            slots,
            slotAssignments,
            new TaskSlotDescription.KafkaTopicDescription(
                taskDesc.getOperation().getKafkaTopic().getBootstrapServersList(),
                taskDesc.getOperation().getKafkaTopic().getUsername(),
                taskDesc.getOperation().getKafkaTopic().getPassword(),
                taskDesc.getOperation().getKafkaTopic().getTopic()
            )
        );

        return new TaskState(
            taskDesc.getId(),
            taskDesc.getOperation().getName(),
            graphState.id(),
            Status.WAITING,
            graphState.executionId(),
            graphState.workflowName(),
            graphState.userId(),
            null,
            taskSlotDescription,
            Collections.emptyList(),
            Collections.emptyList()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskState that = (TaskState) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
