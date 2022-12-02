package ai.lzy.channelmanager.v2.config;

public class ConnectionManagerConfig {

    private int cacheConcurrencyLevel;
    private int cacheTTLSeconds;

    public int getCacheConcurrencyLevel() {
        return cacheConcurrencyLevel;
    }

    public int getCacheTTLSeconds() {
        return cacheTTLSeconds;
    }

    public void setCacheConcurrencyLevel(int cacheConcurrencyLevel) {
        this.cacheConcurrencyLevel = cacheConcurrencyLevel;
    }

    public void setCacheTTLSeconds(int cacheTTLSeconds) {
        this.cacheTTLSeconds = cacheTTLSeconds;
    }


    @Override
    public String toString() {
        return "ConnectionManagerConfig{" +
               "cacheConcurrencyLevel='" + cacheConcurrencyLevel + '\'' +
               ", cacheTTLSeconds='" + cacheTTLSeconds + '\'' +
               '}';
    }
}
