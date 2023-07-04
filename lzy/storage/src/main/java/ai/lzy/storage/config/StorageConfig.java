package ai.lzy.storage.config;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.model.db.DatabaseConfiguration;
import io.micronaut.context.annotation.ConfigurationBuilder;
import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.validation.ValidationException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
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

    @Getter
    @Setter
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

        @Getter
        @Setter
        @ConfigurationProperties("yc")
        public static final class YcS3Credentials {
            private boolean enabled;
            private String endpoint;
            private String accessToken;
            private String secretToken;
        }

        @Getter
        @Setter
        @ConfigurationProperties("azure")
        public static final class AzureS3Credentials {
            private boolean enabled;
            private String connectionString;
        }

        @Getter
        @Setter
        @ConfigurationProperties("memory")
        public static class InMemoryS3Credentials {
            private boolean enabled = false;
            private int port;
        }
    }

    @Getter
    @Setter
    @ConfigurationProperties("yc")
    public static class YcCredentials {
        private boolean enabled = false;
        private String endpoint;
        private String folderId;
    }
}
