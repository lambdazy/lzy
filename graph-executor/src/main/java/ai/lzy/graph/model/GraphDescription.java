package ai.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi;

import java.util.List;
import java.util.stream.Collectors;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.ChannelDesc;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi.TaskDesc;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphDescription(
    List<TaskDescription> tasks,
    Map<String, ChannelDescription> channels // Map from channel id to its description
) {

    public static GraphDescription fromGrpc(List<TaskDesc> tasks, List<ChannelDesc> channels) {

        final List<TaskDescription> taskDescriptions = tasks.stream()
            .map(t -> new TaskDescription(
                t.getId(),
                GrpcConverter.from(t.getZygote()),
                t.getSlotAssignmentsList()
                .stream()
                .collect(Collectors.toMap(
                    GraphExecutorApi.SlotToChannelAssignment::getSlotName,
                    GraphExecutorApi.SlotToChannelAssignment::getChannelId
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
