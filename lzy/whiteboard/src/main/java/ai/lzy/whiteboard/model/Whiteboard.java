package ai.lzy.whiteboard.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Whiteboard that = (Whiteboard) o;
        return id.equals(that.id) && name.equals(that.name) && fields.equals(that.fields) && tags.equals(that.tags) &&
               storage.equals(that.storage) && namespace.equals(that.namespace) && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, fields, tags, storage, namespace, status);
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
