package ai.lzy.whiteboard.model;

import ai.lzy.model.data.DataSchema;
import java.time.Instant;
import java.util.Set;

public record Whiteboard(
    String id,
    String name,
    Set<String> createdFieldNames, // not linked yet
    Set<LinkedField> linkedFields,
    Set<String> tags,
    Storage storage,
    String namespace,
    Status status,
    Instant createdAt
) {

    public boolean hasField(String fieldName) {
        return createdFieldNames.contains(fieldName) || hasLinkedField(fieldName);
    }

    public boolean hasLinkedField(String fieldName) {
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
