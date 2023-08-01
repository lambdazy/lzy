package ai.lzy.storage;

public final class StorageConfig {
    private final S3Credentials s3;
    private final AzureBlobStorageCredentials azure;

    private StorageConfig(S3Credentials s3, AzureBlobStorageCredentials azure) {
        this.s3 = s3;
        this.azure = azure;
    }

    public static StorageConfig of(S3Credentials s3) {
        return new StorageConfig(s3, null);
    }

    public static StorageConfig of(AzureBlobStorageCredentials azure) {
        return new StorageConfig(null, azure);
    }

    public S3Credentials getS3() {
        return s3;
    }

    public AzureBlobStorageCredentials getAzure() {
        return azure;
    }

    public boolean hasS3() {
        return s3 != null;
    }

    public boolean hasAzure() {
        return azure != null;
    }

    public record AzureBlobStorageCredentials(String connectionString) {}

    public record S3Credentials(String endpoint, String accessToken, String secretToken) {}
}
