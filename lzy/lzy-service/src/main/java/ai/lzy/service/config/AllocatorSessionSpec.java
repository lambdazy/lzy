package ai.lzy.service.config;

import ai.lzy.v1.VmAllocatorApi;

public record AllocatorSessionSpec(
    String userId,
    String description,
    VmAllocatorApi.CachePolicy cachePolicy
) {}
