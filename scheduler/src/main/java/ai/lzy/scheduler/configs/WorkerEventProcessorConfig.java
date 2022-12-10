package ai.lzy.scheduler.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("scheduler.worker-processor")
public record WorkerEventProcessorConfig(
        Duration allocationTimeout,
        Duration idleTimeout,
        Duration configuringTimeout,
        Duration workerStopTimeout,
        Duration executingHeartbeatPeriod,
        Duration idleHeartbeatPeriod
) {}
