package ai.lzy.allocator.model;


import jakarta.annotation.Nullable;

public record Session(
    String sessionId,
    String owner,
    @Nullable
    String description,
    CachePolicy cachePolicy,
    String createOpId,
    @Nullable
    String deleteOpId,
    @Nullable
    String deleteReqid
)
{
    public Session(String sessionId, String owner, String description, CachePolicy cachePolicy, String createOpId) {
        this(sessionId, owner, description, cachePolicy, createOpId, null, null);
    }
}
