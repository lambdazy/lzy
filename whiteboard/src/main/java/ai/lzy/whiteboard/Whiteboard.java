package ai.lzy.whiteboard;

import ai.lzy.model.data.DataSchema;
import java.time.Instant;
import java.util.Set;

public record Whiteboard(
    String id,
    Set<String> createdFieldNames,
    Set<LinkedField> linkedFields,
    Set<String> tags,
    Storage storage,
    String namespace,
    Status status,
    Instant createdAt
) {

    public record Storage(
        String name,
        String description
    ) { }

    public record LinkedField(
        String name,
        String storageUri,
        DataSchema schema
    ) { }

    public enum Status {
        CREATED,
        FINALIZED,
        ;
    }

    public boolean hasField(String fieldName) {
        if (createdFieldNames.contains(fieldName)) {
            return true;
        }
        if (linkedFields.stream().anyMatch(f -> f.name().contains(fieldName))) {
            return true;
        }
        return false;
    }
}
