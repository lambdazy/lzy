package ai.lzy.server.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import ai.lzy.model.StorageCredentials;
import ai.lzy.model.StorageCredentials.EmptyCredentials;
import ai.lzy.server.storage.AmazonCredentialsImpl;
import ai.lzy.server.storage.AzureCredentialsImpl;

@ConfigurationProperties("storage")
public class StorageConfigs {

    private AmazonCredentials amazon = new AmazonCredentials();
    private AzureCredentials azure = new AzureCredentials();
    private String bucket;
    private boolean separated;

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

    public StorageCredentials credentials() {
        if (amazon.isEnabled()) {
            return new AmazonCredentialsImpl(amazon.endpoint, amazon.accessToken, amazon.secretToken);
        }
        if (azure.isEnabled()) {
            return new AzureCredentialsImpl(azure.connectionString);
        }
        return new EmptyCredentials();
    }

    @ConfigurationProperties("azure")
    public static class AzureCredentials {
        private String connectionString;

        private boolean enabled;

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
        private boolean enabled;

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
