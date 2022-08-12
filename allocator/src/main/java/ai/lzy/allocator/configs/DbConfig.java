package ai.lzy.allocator.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("allocator.database")
public record DbConfig(
    String url,
    String username,
    String password,
    int minPoolSize,
    int maxPoolSize,
    boolean enabled
) {}
