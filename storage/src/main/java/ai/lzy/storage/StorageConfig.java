package ai.lzy.storage;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;

import javax.validation.ValidationException;

@ConfigurationProperties("storage")
public class StorageConfig {
    private String address;

    @ConfigurationBuilder("iam")
    private final IamClientConfiguration iam = new IamClientConfiguration();

    @ConfigurationBuilder("database")
    private final DatabaseConfiguration database = new DatabaseConfiguration();

    private S3Credentials s3;

    // legacy credentials format
    private YcCredentials yc;

    public void validate() {
        s3.validate();
    }

    public String address() {
        return address;
    }

    public IamClientConfiguration iam() {
        return iam;
    }

    public DatabaseConfiguration database() {
        return database;
    }

    public S3Credentials s3() {
        return s3;
    }

    public YcCredentials yc() {
        return yc;
    }

    @ConfigurationProperties("s3")
    public static class S3Credentials {

        @ConfigurationBuilder("memory")
        private final InMemoryS3Credentials memory = new InMemoryS3Credentials();

        @ConfigurationBuilder("yc")
        private final YcS3Credentials yc = new YcS3Credentials();

        @ConfigurationBuilder("azure")
        private final AzureS3Credentials azure = new AzureS3Credentials();

        public void validate() {
            int cnt = (yc.enabled ? 1 : 0)
                + (azure.enabled ? 1 : 0)
                + (memory.enabled ? 1 : 0);

            if (cnt != 1) {
                throw new ValidationException("Exactly one s3 provider should be enabled.");
            }
        }

        public YcS3Credentials yc() {
            return yc;
        }

        public AzureS3Credentials azure() {
            return azure;
        }

        public InMemoryS3Credentials memory() {
            return memory;
        }

        public InMemoryS3Credentials getMemory() {
            return memory;
        }

        public YcS3Credentials getYc() {
            return yc;
        }

        public AzureS3Credentials getAzure() {
            return azure;
        }

        @ConfigurationProperties("yc")
        public static final class YcS3Credentials {
            private boolean enabled;
            private String endpoint;
            private String accessToken;
            private String secretToken;

            public boolean enabled() {
                return enabled;
            }

            public String endpoint() {
                return endpoint;
            }

            public String accessToken() {
                return accessToken;
            }

            public String secretToken() {
                return secretToken;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public void setEndpoint(String endpoint) {
                this.endpoint = endpoint;
            }

            public void setAccessToken(String accessToken) {
                this.accessToken = accessToken;
            }

            public void setSecretToken(String secretToken) {
                this.secretToken = secretToken;
            }
        }

        @ConfigurationProperties("azure")
        public static final class AzureS3Credentials {
            private boolean enabled;
            private String connectionString;

            public boolean enabled() {
                return enabled;
            }

            public String connectionString() {
                return connectionString;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public void setConnectionString(String connectionString) {
                this.connectionString = connectionString;
            }
        }

        @ConfigurationProperties("memory")
        public static class InMemoryS3Credentials {
            private boolean enabled = false;
            private int port;

            public boolean enabled() {
                return enabled;
            }

            public int port() {
                return port;
            }

            public InMemoryS3Credentials setEnabled(boolean enabled) {
                this.enabled = enabled;
                return this;
            }

            public InMemoryS3Credentials setPort(int port) {
                this.port = port;
                return this;
            }
        }
    }

    @ConfigurationProperties("yc")
    public static class YcCredentials {
        private boolean enabled = false;
        private String serviceAccountId;
        private String keyId;
        private String privateKey;
        private String folderId;

        public boolean enabled() {
            return enabled;
        }

        public String serviceAccountId() {
            return serviceAccountId;
        }

        public String keyId() {
            return keyId;
        }

        public String privateKey() {
            return privateKey;
        }

        public String folderId() {
            return folderId;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public void setServiceAccountId(String serviceAccountId) {
            this.serviceAccountId = serviceAccountId;
        }

        public void setKeyId(String keyId) {
            this.keyId = keyId;
        }

        public void setPrivateKey(String privateKey) {
            this.privateKey = privateKey;
        }

        public void setFolderId(String folderId) {
            this.folderId = folderId;
        }
    }

    public IamClientConfiguration getIam() {
        return iam;
    }

    public DatabaseConfiguration getDatabase() {
        return database;
    }

    public S3Credentials getS3() {
        return s3;
    }

    public YcCredentials getYc() {
        return yc;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setS3(S3Credentials s3) {
        this.s3 = s3;
    }

    public void setYc(YcCredentials yc) {
        this.yc = yc;
    }
}
