package ai.lzy.allocator.model;

import javax.annotation.Nullable;

public record Session(
    String sessionId,
    String owner,
    @Nullable
    String description,
    CachePolicy cachePolicy,
    String opId
) {}
