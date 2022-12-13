package ai.lzy.channelmanager.config;

import java.time.Duration;

public class ConnectionManagerConfig {

    private int cacheConcurrencyLevel;
    private Duration cacheTTL;

    public int getCacheConcurrencyLevel() {
        return cacheConcurrencyLevel;
    }

    public Duration getCacheTTL() {
        return cacheTTL;
    }

    public void setCacheConcurrencyLevel(int cacheConcurrencyLevel) {
        this.cacheConcurrencyLevel = cacheConcurrencyLevel;
    }

    public void setCacheTTL(int cacheTTLSeconds) {
        this.cacheTTL = Duration.ofSeconds(cacheTTLSeconds);
    }

    @Override
    public String toString() {
        return "ConnectionManagerConfig{" +
               "cacheConcurrencyLevel='" + cacheConcurrencyLevel + '\'' +
               ", cacheTTLSeconds='" + cacheTTL + '\'' +
               '}';
    }
}
