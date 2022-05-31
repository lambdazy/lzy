package ru.yandex.cloud.ml.platform.lzy.graph_executor.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskDescription(
        String id,
        @JsonSerialize(using = ZygoteSerializer.class)
        @JsonDeserialize(using = ZygoteDeserializer.class)
        Zygote zygote,
        Map<String, String> slotsToChannelsAssignments
) {}
