package ai.lzy.allocator.model;


import jakarta.annotation.Nullable;

public record Session(
    String sessionId,
    String owner,
    @Nullable
    String description,
    CachePolicy cachePolicy,
    String allocateOpId,
    @Nullable
    String deleteOpId
) {}
