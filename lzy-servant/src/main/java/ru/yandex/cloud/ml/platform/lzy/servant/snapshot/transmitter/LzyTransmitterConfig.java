package ru.yandex.cloud.ml.platform.lzy.servant.snapshot.transmitter;


public class LzyTransmitterConfig {
    private LzyTransmitterConfig(String access, String secret, String region, String endpoint, String pathStyleAccessEnabled) {
        this.access = access;
        this.secret = secret;
        this.region = region;
        this.endpoint = endpoint;
        this.pathStyleAccessEnabled = Boolean.parseBoolean(pathStyleAccessEnabled);
    }

    private final String access;
    private final String secret;
    private final String region;
    private final String endpoint;
    private final boolean pathStyleAccessEnabled;

    public static LzyTransmitterConfigBuilder builder() {
        return new LzyTransmitterConfigBuilder();
    }

    public String getAccess() {
        return access;
    }

    public String getSecret() {
        return secret;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isPathStyleAccessEnabled() {
        return pathStyleAccessEnabled;
    }

    public static class LzyTransmitterConfigBuilder {
        private String access;
        private String secret;
        private String region;
        private String endpoint;
        private String pathStyleAccessEnabled;

        public LzyTransmitterConfigBuilder access(String access) {
            this.access = access;
            return this;
        }

        public LzyTransmitterConfigBuilder secret(String secret) {
            this.secret = secret;
            return this;
        }

        public LzyTransmitterConfigBuilder region(String region) {
            this.region = region;
            return this;
        }

        public LzyTransmitterConfigBuilder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public LzyTransmitterConfigBuilder pathStyleAccessEnabled(String pathStyleAccessEnabled) {
            this.pathStyleAccessEnabled = pathStyleAccessEnabled;
            return this;
        }

        public LzyTransmitterConfig build() {
            return new LzyTransmitterConfig(access, secret, region, endpoint, pathStyleAccessEnabled);
        }
    }
}
