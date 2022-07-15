package ai.lzy.scheduler.configs;

import java.time.Duration;

public class ProcessorConfigBuilder {
    private Duration allocationTimeout = Duration.ofDays(100);
    private Duration idleTimeout = Duration.ofDays(100);
    private Duration configuringTimeout = Duration.ofDays(100);
    private Duration servantStopTimeout = Duration.ofDays(100);
    private Duration executingHeartbeatPeriod = Duration.ofDays(100);
    private Duration idleHeartbeatPeriod = Duration.ofDays(100);

    public ProcessorConfigBuilder setAllocationTimeout(long millis) {
        this.allocationTimeout = Duration.ofMillis(millis);
        return this;
    }

    public ProcessorConfigBuilder setIdleTimeout(long millis) {
        this.idleTimeout = Duration.ofMillis(millis);
        return this;
    }

    public ProcessorConfigBuilder setConfiguringTimeout(long millis) {
        this.configuringTimeout = Duration.ofMillis(millis);
        return this;
    }

    public ProcessorConfigBuilder setServantStopTimeout(long millis) {
        this.servantStopTimeout = Duration.ofMillis(millis);
        return this;
    }

    public ProcessorConfigBuilder setExecutingHeartbeatPeriod(long millis) {
        this.executingHeartbeatPeriod = Duration.ofMillis(millis);
        return this;
    }

    public ProcessorConfigBuilder setIdleHeartbeatPeriod(long millis) {
        this.idleHeartbeatPeriod = Duration.ofMillis(millis);
        return this;
    }

    public ServantEventProcessorConfig build() {
        return new ServantEventProcessorConfig(allocationTimeout, idleTimeout, configuringTimeout,
                servantStopTimeout, executingHeartbeatPeriod, idleHeartbeatPeriod);
    }
}
