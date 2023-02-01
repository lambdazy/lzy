package ai.lzy.graph.model;

import ai.lzy.model.operation.Operation;
import ai.lzy.v1.graph.GraphExecutor.ChannelDesc;
import ai.lzy.v1.graph.GraphExecutor.SlotToChannelAssignment;
import ai.lzy.v1.graph.GraphExecutor.TaskDesc;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphDescription(
    List<TaskDescription> tasks,
    Map<String, ChannelDescription> channels // Map from channel id to its description
)
{

    public static GraphDescription fromGrpc(List<TaskDesc> tasks, List<ChannelDesc> channels) {

        final List<TaskDescription> taskDescriptions = tasks.stream()
            .map(t -> new TaskDescription(
                t.getId(),
                Operation.fromProto(t.getOperation()),
                t.getSlotAssignmentsList()
                    .stream()
                    .collect(Collectors.toMap(
                        SlotToChannelAssignment::getSlotName,
                        SlotToChannelAssignment::getChannelId,
                        (ch1, ch2) -> ch1 // ignore duplicates, because multiple arguments can use the same slot/channel
                    ))))
            .collect(Collectors.toList());

        final Map<String, ChannelDescription> channelDescriptions = channels
            .stream()
            .map(t -> new ChannelDescription(
                ChannelDescription.Type.valueOf(t.getTypeCase().name()),
                t.getId()
            ))
            .collect(Collectors.toMap(ChannelDescription::id, t -> t));

        return new GraphDescription(taskDescriptions, channelDescriptions);
    }

}
