package ai.lzy.storage;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.validation.ValidationException;

@ConfigurationProperties("storage")
public record StorageConfig(
    AmazonCredentials amazon,
    AzureCredentials azure,
    InMemoryCredentials memory
) {

    public void validate() {
        int cnt = (amazon.enabled ? 1 : 0)
            + (azure.enabled ? 1 : 0)
            + (memory.enabled ? 1 : 0);

        if (cnt != 1) {
            throw new ValidationException("Exactly one s3 provider should be enabled.");
        }
    }

    public record AmazonCredentials(
        boolean enabled,
        String endpoint,
        String accessToken,
        String secretToken
    ) {}

    public record AzureCredentials(
        boolean enabled,
        String connectionString
    ) {}

    public record InMemoryCredentials(
        boolean enabled
    ) {}
}
