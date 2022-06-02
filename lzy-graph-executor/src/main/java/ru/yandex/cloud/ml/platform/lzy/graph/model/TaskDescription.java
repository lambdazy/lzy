package ru.yandex.cloud.ml.platform.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;

import java.util.Map;
import ru.yandex.cloud.ml.platform.lzy.model.json.ZygoteDeserializer;
import ru.yandex.cloud.ml.platform.lzy.model.json.ZygoteSerializer;

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
