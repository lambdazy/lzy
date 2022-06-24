package ai.lzy.kharon.workflow.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.annotation.Nullable;

@ConfigurationProperties("kharon.workflow.database")
public record WorkflowDatabaseConfig(
    @Nullable
    String url,
    @Nullable
    String username,
    @Nullable
    String password,
    @Nullable
    Integer minPoolSize,
    @Nullable
    Integer maxPoolSize
) {}
