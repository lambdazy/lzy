package ru.yandex.cloud.ml.platform.lzy.graph_executor.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import yandex.cloud.priv.datasphere.v2.lzy.GraphExecutorApi;

import java.util.List;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record GraphDescription(List<TaskDescription> tasks) {
    public static GraphDescription fromGrpc(List<GraphExecutorApi.TaskDesc> tasks) {
        List<TaskDescription> taskDescriptions = tasks.stream()
            .map( t -> new TaskDescription(
                t.getId(),
                GrpcConverter.from(t.getZygote()),
                t.getSlotAssignmentsList()
                .stream()
                .collect(Collectors.toMap(
                    GraphExecutorApi.SlotToChannelAssignment::getSlotName,
                    GraphExecutorApi.SlotToChannelAssignment::getChannelId
                    )
                )
            )
        ).collect(Collectors.toList());
        return new GraphDescription(taskDescriptions);
    }
}
