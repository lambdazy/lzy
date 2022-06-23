package ai.lzy.kharon.workflow.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.annotation.Nullable;

@ConfigurationProperties("kharon.workflow")
public record WorkflowServiceConfig(
    @Nullable
    String serverAddress,
    @Nullable
    WorkflowDatabaseConfig database
) {}
