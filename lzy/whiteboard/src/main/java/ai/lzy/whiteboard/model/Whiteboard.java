package ai.lzy.whiteboard.model;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

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
    public enum Status {
        CREATED,
        FINALIZED,
    }

    public record Storage(
        String name,
        String description,
        URI uri
    )
    {
    }
}
