package ai.lzy.graph.model;

import ai.lzy.v1.common.LMO;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hubspot.jackson.datatype.protobuf.builtin.serializers.MessageSerializer;

import java.util.Map;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record TaskDescription(
        String id,
        @JsonSerialize(using = MessageSerializer.class)
        @JsonDeserialize(using = OperationDeserializer.class)
        LMO.Operation operation,
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
