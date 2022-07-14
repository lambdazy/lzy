package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("servant-processor")
public record ServantEventProcessorConfig(
        float allocationTimeoutSeconds,
        float idleTimeoutSeconds,
        float configuringTimeoutSeconds,
        float servantStopTimeoutSeconds,
        float executingHeartbeatPeriodSeconds,
        float idleHeartbeatPeriodSeconds) { }
