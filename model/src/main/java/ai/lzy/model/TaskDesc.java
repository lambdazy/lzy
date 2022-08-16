package ai.lzy.model;

import ai.lzy.model.json.OperationDeserializer;
import ai.lzy.model.json.OperationSerializer;
import ai.lzy.v1.Tasks;
import ai.lzy.v1.common.LzyCommon;
import ai.lzy.v1.common.LzyCommon.SlotToChannelAssignment;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskDesc(
    @JsonSerialize(using = OperationSerializer.class)
    @JsonDeserialize(using = OperationDeserializer.class)
    Operation operation,
    Map<String, String> slotsToChannelsAssignments
) {
    public static TaskDesc from(LzyCommon.TaskDesc taskDesc) {
        Map<String, String> slotMapping = taskDesc.getSlotAssignmentsList()
            .stream()
            .collect(Collectors.toMap(SlotToChannelAssignment::getSlotName, SlotToChannelAssignment::getChannelId));

        return new TaskDesc(Operation.from(taskDesc.getOperation()), slotMapping);
    }

    public LzyCommon.TaskDesc to() {
        LzyCommon.TaskDesc.Builder builder = LzyCommon.TaskDesc.newBuilder()
            .setOperation(operation.to());
        operation.slots().forEach(slot -> {
            if (Stream.of(Slot.STDOUT, Slot.STDERR)
                .map(Slot::name)
                .noneMatch(s -> s.equals(slot.name()))) {
                builder.addSlotAssignmentsBuilder()
                    .setSlotName(slot.name())
                    .setChannelId(slotsToChannelsAssignments.get(slot.name()))
                    .build();
            }
        });
        return builder.build();
    }
}
