package ai.lzy.graph.model;

import ai.lzy.graph.LGE;
import ai.lzy.v1.common.LMD;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.common.LMS;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


public final class TaskState {
    private final String id;
    private final String name;
    private final String operationId;
    private final String graphId;
    private Status status;
    private final String executionId;
    private final String workflowName;
    private final String userId;
    private final String allocatorSessionId;
    private final TaskSlotDescription taskSlotDescription;
    private final List<String> tasksDependsOn;
    private final List<String> tasksDependsFrom;
    @Nullable
    private ExecutingState executingState;
    @Nullable
    private String errorDescription;

    public TaskState(String id, String name, String operationId, String graphId, Status status, String executionId,
                     String workflowName, String userId, String allocatorSessionId, TaskSlotDescription taskSlotDescr,
                     List<String> tasksDependsOn, List<String> tasksDependsFrom,
                     @Nullable ExecutingState executingState, @Nullable String errorDescription)
    {
        this.id = id;
        this.name = name;
        this.operationId = operationId;
        this.graphId = graphId;
        this.status = status;
        this.executionId = executionId;
        this.workflowName = workflowName;
        this.userId = userId;
        this.allocatorSessionId = allocatorSessionId;
        this.taskSlotDescription = taskSlotDescr;
        this.tasksDependsOn = tasksDependsOn;
        this.tasksDependsFrom = tasksDependsFrom;
        this.executingState = executingState;
        this.errorDescription = errorDescription;
    }

    public enum Status {
        WAITING, WAITING_ALLOCATION, ALLOCATING, EXECUTING, COMPLETED, FAILED;

        public boolean finished() {
            return this == COMPLETED || this == FAILED;
        }
    }

    public record ExecutingState(
        String opId,
        @Nullable String allocOperationId,
        @Nullable String vmId,
        @Nullable Boolean fromCache,
        @Nullable String workerHost,
        int workerPort,
        @Nullable String workerOperationId
    ) {}

    public static TaskState fromProto(LGE.ExecuteGraphRequest.TaskDesc task, GraphState initialGraphState) {
        var slots = task.getOperation().getSlotsList().stream()
            .map(slot -> new TaskSlotDescription.Slot(
                slot.getName(),
                TaskSlotDescription.Slot.Media.valueOf(slot.getMedia().name()),
                TaskSlotDescription.Slot.Direction.valueOf(slot.getDirection().name()),
                slot.getContentType().getDataFormat(),
                slot.getContentType().getSchemeFormat(),
                slot.getContentType().getSchemeContent(),
                slot.getContentType().getMetadataMap()))
            .toList();

        var slotAssignments = task.getSlotAssignmentsList().stream()
            .collect(Collectors.toMap(
                LGE.ExecuteGraphRequest.TaskDesc.SlotToChannelAssignment::getSlotName,
                LGE.ExecuteGraphRequest.TaskDesc.SlotToChannelAssignment::getChannelId));

        var taskSlotDescription = new TaskSlotDescription(
            task.getOperation().getName(),
            task.getOperation().getDescription(),
            task.getOperation().getRequirements().getPoolLabel(),
            task.getOperation().getRequirements().getZone(),
            task.getOperation().getCommand(),
            slots,
            slotAssignments,
            new TaskSlotDescription.KafkaTopicDescription(
                task.getOperation().getKafkaTopic().getBootstrapServersList(),
                task.getOperation().getKafkaTopic().getUsername(),
                task.getOperation().getKafkaTopic().getPassword(),
                task.getOperation().getKafkaTopic().getTopic()));

        return new TaskState(
            task.getId(),
            task.getOperation().getName(),
            initialGraphState.operationId(),
            initialGraphState.id(),
            Status.WAITING,
            initialGraphState.executionId(),
            initialGraphState.workflowName(),
            initialGraphState.userId(),
            initialGraphState.allocatorSessionId(),
            taskSlotDescription,
            new ArrayList<>(),
            new ArrayList<>(),
            null,
            null);
    }

    public LMO.TaskDesc toProto() {
        return LMO.TaskDesc.newBuilder()
            .addAllSlotAssignments(
                taskSlotDescription.slotsToChannelsAssignments().entrySet().stream()
                    .map(e -> LMO.SlotToChannelAssignment.newBuilder()
                        .setSlotName(e.getKey())
                        .setChannelId(e.getValue())
                        .build())
                    .toList())
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
                    .build())
            .build();
    }

    public LGE.TaskExecutionStatus toProtoStatus() {
        var builder = LGE.TaskExecutionStatus.newBuilder()
            .setTaskId(id)
            .setTaskDescriptionId(id)
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setOperationName(name);

        return switch (status) {
            case COMPLETED ->
                builder.setSuccess(
                    LGE.TaskExecutionStatus.Success.newBuilder()
                        .setRc(0)
                        .setDescription("")
                        .build())
                    .build();
            case FAILED ->
                builder.setError(
                    LGE.TaskExecutionStatus.Error.newBuilder()
                        .setRc(1)
                        .setDescription(requireNonNull(errorDescription))
                        .build())
                    .build();
            case WAITING, WAITING_ALLOCATION, ALLOCATING, EXECUTING ->
                builder
                    .setExecuting(
                        LGE.TaskExecutionStatus.Executing.newBuilder()
                            .build())
                    .build();
        };
    }

    public TaskState toWaitAllocation(String execTaskOpId) {
        return new TaskState(id, name, operationId, graphId, Status.WAITING_ALLOCATION, executionId, workflowName,
            userId, allocatorSessionId, taskSlotDescription, tasksDependsOn, tasksDependsFrom,
            new ExecutingState(execTaskOpId, null, null, null, null, -1, null), null);
    }

    public TaskState toStartAllocation(String allocOpId, String vmId) {
        Objects.requireNonNull(executingState);
        return new TaskState(id, name, operationId, graphId, Status.ALLOCATING, executionId, workflowName,
            userId, allocatorSessionId, taskSlotDescription, tasksDependsOn, tasksDependsFrom,
            new ExecutingState(executingState.opId, allocOpId, vmId, null, null, -1, null), null);
    }

    public TaskState toExecutingState(String vmHost, int vmPort, boolean fromCache) {
        Objects.requireNonNull(executingState);
        return new TaskState(id, name, operationId, graphId, Status.EXECUTING, executionId, workflowName,
            userId, allocatorSessionId, taskSlotDescription, tasksDependsOn, tasksDependsFrom,
            new ExecutingState(executingState.opId, executingState.allocOperationId, executingState.vmId,
                fromCache, vmHost, vmPort, null), null);
    }

    public TaskState toExecutingState(String workerOpId) {
        Objects.requireNonNull(executingState);
        assert status == Status.EXECUTING;
        return new TaskState(id, name, operationId, graphId, status, executionId, workflowName,
            userId, allocatorSessionId, taskSlotDescription, tasksDependsOn, tasksDependsFrom,
            new ExecutingState(executingState.opId, executingState.allocOperationId, executingState.vmId,
                executingState.fromCache, executingState.workerHost, executingState.workerPort, workerOpId), null);
    }

    public void complete() {
        status = Status.COMPLETED;
    }

    public void fail(String errorDescription) {
        status = Status.FAILED;
        this.errorDescription = errorDescription;
    }

    public String id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String operationId() {
        return operationId;
    }

    public String graphId() {
        return graphId;
    }

    public Status status() {
        return status;
    }

    public String executionId() {
        return executionId;
    }

    public String workflowName() {
        return workflowName;
    }

    public String userId() {
        return userId;
    }

    @Nullable
    public String errorDescription() {
        return errorDescription;
    }

    public TaskSlotDescription taskSlotDescription() {
        return taskSlotDescription;
    }

    public String allocatorSessionId() {
        return allocatorSessionId;
    }

    @Nullable
    public ExecutingState executingState() {
        return executingState;
    }

    public List<String> tasksDependedOn() {
        return tasksDependsOn;
    }

    public List<String> tasksDependedFrom() {
        return tasksDependsFrom;
    }

    @Override
    public String toString() {
        return "TaskState[" +
            "id=" + id + ", " +
            "name=" + name + ", " +
            "operationId=" + operationId + ", " +
            "graphId=" + graphId + ", " +
            "status=" + status + ", " +
            "executionId=" + executionId + ", " +
            "workflowName=" + workflowName + ", " +
            "userId=" + userId + ", " +
            "errorDescription=" + errorDescription + ", " +
            "taskSlotDescription=" + taskSlotDescription + ", " +
            "allocatorSessionId=" + allocatorSessionId + ", " +
            "executingState=" + executingState + ", " +
            "tasksDependedOn=" + tasksDependsOn + ", " +
            "tasksDependedFrom=" + tasksDependsFrom + ']';
    }

}
