package ai.lzy.graph.model;

import ai.lzy.model.Operation;
import ai.lzy.model.json.OperationDeserializer;
import ai.lzy.model.json.OperationSerializer;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Objects;
import ai.lzy.model.Zygote;

import java.util.Map;
import ai.lzy.model.json.ZygoteDeserializer;
import ai.lzy.model.json.ZygoteSerializer;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskDescription(
        String id,
        @JsonSerialize(using = OperationSerializer.class)
        @JsonDeserialize(using = OperationDeserializer.class)
        Operation operation,
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
