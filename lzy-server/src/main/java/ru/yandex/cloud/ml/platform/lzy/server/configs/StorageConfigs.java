package ru.yandex.cloud.ml.platform.lzy.server.configs;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("storage")
public class StorageConfigs {

    private AmazonCredentials amazon = new AmazonCredentials();
    private AzureCredentials azure = new AzureCredentials();
    private String bucket;
    private boolean separated = false;

    public boolean isSeparated() {
        return separated;
    }

    public void setSeparated(boolean separated) {
        this.separated = separated;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public AmazonCredentials getAmazon() {
        return amazon;
    }

    public void setAmazon(AmazonCredentials amazon) {
        this.amazon = amazon;
    }

    public AzureCredentials getAzure() {
        return azure;
    }

    public void setAzure(AzureCredentials azure) {
        this.azure = azure;
    }

    @ConfigurationProperties("azure")
    public static class AzureCredentials {
        private String connectionString;

        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getConnectionString() {
            return connectionString;
        }

        public void setConnectionString(String connectionString) {
            this.connectionString = connectionString;
        }
    }

    @ConfigurationProperties("amazon")
    public static class AmazonCredentials {
        private String endpoint;
        private String accessToken;
        private String secretToken;
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public void setAccessToken(String accessToken) {
            this.accessToken = accessToken;
        }

        public String getSecretToken() {
            return secretToken;
        }

        public void setSecretToken(String secretToken) {
            this.secretToken = secretToken;
        }
    }
}
