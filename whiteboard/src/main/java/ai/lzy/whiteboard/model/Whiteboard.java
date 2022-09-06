package ai.lzy.whiteboard.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public record Whiteboard(
    String id,
    String name,
    Map<String, Field> fields,
    Set<String> tags,
    Storage storage,
    String namespace,
    Status status,
    Instant createdAt
) {

    @Nullable
    public Field getField(String name) {
        return fields.get(name);
    }

    public List<Field> unlinkedFields() {
        return fields.values().stream()
            .filter(f -> !(f instanceof LinkedField))
            .collect(Collectors.toList());
    }

    public List<LinkedField> linkedFields() {
        return fields.values().stream()
            .filter(f -> f instanceof LinkedField)
            .map(f -> (LinkedField) f)
            .collect(Collectors.toList());
    }

    public enum Status {
        CREATED,
        FINALIZED,
    }

    public record Storage(
        String name,
        String description
    ) { }

}
