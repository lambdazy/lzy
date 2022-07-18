package ai.lzy.storage;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.validation.ValidationException;

@ConfigurationProperties("storage")
public record StorageConfig(
    String address,
    StorageDataSourceConfig database,
    S3Credentials s3,
    YcCredentials yc
) {

    public void validate() {
        s3.validate();
    }

    @ConfigurationProperties("s3")
    public record S3Credentials(
        YcS3Credentials yc,
        AzureS3Credentials azure,
        InMemoryS3Credentials memory
    ) {
        public void validate() {
            int cnt = (yc.enabled ? 1 : 0)
                + (azure.enabled ? 1 : 0)
                + (memory.enabled ? 1 : 0);

            if (cnt != 1) {
                throw new ValidationException("Exactly one s3 provider should be enabled.");
            }
        }
    }

    @ConfigurationProperties("s3.yc")
    public record YcS3Credentials(
        boolean enabled,
        String endpoint,
        String accessToken,
        String secretToken
    ) {}

    @ConfigurationProperties("s3.azure")
    public record AzureS3Credentials(
        boolean enabled,
        String connectionString
    ) {}

    @ConfigurationProperties("s3.memory")
    public record InMemoryS3Credentials(
        boolean enabled,
        int port
    ) {}

    @ConfigurationProperties("database")
    public record StorageDataSourceConfig(
        String url,
        String username,
        String password,
        int minPoolSize,
        int maxPoolSize
    ) {}

    @ConfigurationProperties("yc")
    public record YcCredentials(
        boolean enabled,
        String serviceAccountId,
        String keyId,
        String privateKey,
        String folderId
    ) {}
}
