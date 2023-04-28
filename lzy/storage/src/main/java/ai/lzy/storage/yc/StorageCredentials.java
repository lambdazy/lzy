package ai.lzy.storage.yc;

import jakarta.annotation.Nullable;

public interface StorageCredentials {

    enum Type {
        AmazonS3,
        Azure
    }

    Type type();

    record AmazonS3Credentials(
        String endpoint,
        @Nullable
        String accessToken,
        @Nullable
        String secretToken
    ) implements StorageCredentials {
        @Override
        public Type type() {
            return Type.AmazonS3;
        }
    }

    record AzureCredentials(
        String connectionString
    ) implements StorageCredentials {
        @Override
        public Type type() {
            return Type.Azure;
        }
    }
}
