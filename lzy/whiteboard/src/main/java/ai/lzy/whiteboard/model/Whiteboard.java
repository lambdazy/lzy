package ai.lzy.whiteboard.model;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
)
{
    @Nullable
    public Field getField(String name) {
        return fields.get(name);
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
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public enum Status {
        CREATED,
        FINALIZED,
    }

    public record Storage(
        String name,
        String description,
        URI uri
    ) { }
}
