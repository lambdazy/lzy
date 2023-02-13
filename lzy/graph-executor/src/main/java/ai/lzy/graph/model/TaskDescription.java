package ai.lzy.graph.model;

import ai.lzy.v1.common.LMO;

import java.util.Map;
import java.util.Objects;

public record TaskDescription(
        String id,
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
