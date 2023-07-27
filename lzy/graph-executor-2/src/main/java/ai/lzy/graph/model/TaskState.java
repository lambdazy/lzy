package ai.lzy.graph.model;

import ai.lzy.graph.GraphExecutorApi2;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Builder(toBuilder = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskState(
    String id,
    String name,
    String operationId,
    String graphId,
    Status status,
    String executionId,
    String workflowName,
    String userId,
    String errorDescription,
    TaskSlotDescription taskSlotDescription,
    String allocatorSession,
    ExecutingState executingState,
    List<String> tasksDependedOn, // tasks, on which this task is depended on
    List<String> tasksDependedFrom // tasks, that are depended from this task
) {
    public enum Status {
        WAITING, WAITING_ALLOCATION, ALLOCATING, EXECUTING, COMPLETED, FAILED
    }

    @Builder(toBuilder = true)
    public record ExecutingState(
        String opId,
        String allocOperationId,
        String vmId,
        boolean fromCache,
        String workerHost,
        int workerPort,
        String workerOperationId
    ) {}

    public static TaskState fromProto(GraphExecutorApi2.GraphExecuteRequest.TaskDesc taskDesc, GraphState graphState) {
        var slots = taskDesc.getOperation().getSlotsList().stream()
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
        var slotAssignments = taskDesc.getSlotAssignmentsList().stream()
            .collect(Collectors.toMap(t -> t.getSlotName(), t -> t.getChannelId()));
        var taskSlotDescription = new TaskSlotDescription(
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
            graphState.operationId(),
            graphState.id(),
            Status.WAITING,
            graphState.executionId(),
            graphState.workflowName(),
            graphState.userId(),
            null,
            taskSlotDescription,
            null,
            ExecutingState.builder().build(),
            new ArrayList<>(),
            new ArrayList<>()
        );
    }

    public LMO.TaskDesc toProto() {
        return LMO.TaskDesc.newBuilder()
            .addAllSlotAssignments(
                taskSlotDescription.slotsToChannelsAssignments().entrySet().stream()
                    .map(e -> LMO.SlotToChannelAssignment.newBuilder()
                        .setSlotName(e.getKey())
                        .setChannelId(e.getValue())
                        .build())
                    .toList()
            )
            .setOperation(
                LMO.Operation.newBuilder()
                    .setName(taskSlotDescription.name())
                    .setDescription(taskSlotDescription.description())
                    .setCommand(taskSlotDescription.command())
                    .setRequirements(LMO.Requirements.newBuilder()
                        .setZone(taskSlotDescription.zone())
                        .setPoolLabel(taskSlotDescription.poolLabel())
                        .build())
                    .setKafkaTopic(LMO.KafkaTopicDescription.newBuilder()
                        .setUsername(taskSlotDescription.stdLogsKafkaTopic().username())
                        .setPassword(taskSlotDescription.stdLogsKafkaTopic().password())
                        .setTopic(taskSlotDescription.stdLogsKafkaTopic().topic())
                        .addAllBootstrapServers(taskSlotDescription.stdLogsKafkaTopic().bootstrapServers())
                        .build())
                    .addAllSlots(taskSlotDescription.slots().stream()
                        .map(slot -> LMS.Slot.newBuilder()
                            .setName(slot.name())
                            .setMedia(LMS.Slot.Media.valueOf(slot.media().name()))
                            .setDirection(LMS.Slot.Direction.valueOf(slot.direction().name()))
                            .setContentType(LMD.DataScheme.newBuilder()
                                .setDataFormat(slot.dataFormat())
                                .setSchemeFormat(slot.schemeContent())
                                .setSchemeContent(slot.schemeContent())
                                .putAllMetadata(slot.metadata())
                                .build())
                            .build())
                        .toList())
                    .build()
            )
            .build();
    }

    public GraphExecutorApi2.TaskExecutionStatus toProtoStatus() {
        GraphExecutorApi2.TaskExecutionStatus.Builder builder = GraphExecutorApi2.TaskExecutionStatus.newBuilder()
            .setTaskDescriptionId(id)
            .setTaskId(id)
            .setOperationName(name)
            .setWorkflowId(executionId);

        switch (status) {
            case COMPLETED -> builder.setSuccess(GraphExecutorApi2.TaskExecutionStatus.Success.newBuilder()
                .setRc(0)
                .setDescription("")
                .build());
            case FAILED -> builder.setError(GraphExecutorApi2.TaskExecutionStatus.Error.newBuilder()
                .setRc(0)
                .setDescription(errorDescription)
                .build());
            default -> builder.setExecuting(GraphExecutorApi2.TaskExecutionStatus.Executing.newBuilder().build());
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
        TaskState that = (TaskState) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
