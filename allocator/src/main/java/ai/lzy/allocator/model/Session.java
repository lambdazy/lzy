package ai.lzy.allocator.model;

public record Session(
    String sessionId,
    String owner,
    CachePolicy cachePolicy
) {}
