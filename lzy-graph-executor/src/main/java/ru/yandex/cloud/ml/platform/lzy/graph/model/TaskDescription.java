package ru.yandex.cloud.ml.platform.lzy.graph.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Objects;
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
) {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskDescription that = (TaskDescription) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
