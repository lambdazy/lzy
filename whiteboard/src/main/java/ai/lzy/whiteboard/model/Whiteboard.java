package ai.lzy.whiteboard.model;

import ai.lzy.model.data.DataSchema;
import java.time.Instant;
import java.util.Set;

public record Whiteboard(
    String id,
    String name,
    Set<String> createdFieldNames,
    Set<LinkedField> linkedFields,
    Set<String> tags,
    Storage storage,
    String namespace,
    Status status,
    Instant createdAt
) {

    public boolean hasField(String fieldName) {
        if (createdFieldNames.contains(fieldName)) {
            return true;
        }
        return linkedFields.stream().anyMatch(f -> f.name().contains(fieldName));
    }

    public enum Status {
        CREATED,
        FINALIZED,
    }

    public record Storage(
        String name,
        String description
    ) { }

    public record LinkedField(
        String name,
        String storageUri,
        DataSchema schema
    ) { }
}
