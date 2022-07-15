package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("servant-processor")
public record ServantEventProcessorConfig(
        Duration allocationTimeout,
        Duration idleTimeout,
        Duration configuringTimeout,
        Duration servantStopTimeout,
        Duration executingHeartbeatPeriod,
        Duration idleHeartbeatPeriod) {

}
