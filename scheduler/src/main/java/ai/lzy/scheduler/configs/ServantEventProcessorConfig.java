package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("servant-processor")
public record ServantEventProcessorConfig(
    int allocationTimeoutSeconds, int idleTimeoutSeconds, int servantStopTimeoutSeconds) {
}
