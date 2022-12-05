package ai.lzy.channelmanager.v2.config;

public class ConnectionManagerConfig {

    private int cacheConcurrencyLevel;
    private int cacheTtlSeconds;

    public int getCacheConcurrencyLevel() {
        return cacheConcurrencyLevel;
    }

    public int getCacheTtlSeconds() {
        return cacheTtlSeconds;
    }

    public void setCacheConcurrencyLevel(int cacheConcurrencyLevel) {
        this.cacheConcurrencyLevel = cacheConcurrencyLevel;
    }

    public void setCacheTtlSeconds(int cacheTtlSeconds) {
        this.cacheTtlSeconds = cacheTtlSeconds;
    }


    @Override
    public String toString() {
        return "ConnectionManagerConfig{" +
               "cacheConcurrencyLevel='" + cacheConcurrencyLevel + '\'' +
               ", cacheTTLSeconds='" + cacheTtlSeconds + '\'' +
               '}';
    }
}
