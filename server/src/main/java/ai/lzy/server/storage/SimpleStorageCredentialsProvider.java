package ai.lzy.server.storage;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import ai.lzy.model.StorageCredentials;
import ai.lzy.model.StorageCredentials.EmptyCredentials;

@Singleton
@Requires(property = "storage.amazon.enabled", value = "false", defaultValue = "false")
@Requires(property = "storage.azure.enabled", value = "false", defaultValue = "false")
public class SimpleStorageCredentialsProvider implements StorageCredentialsProvider {
    @Override
    public StorageCredentials storageCredentials() {
        return new EmptyCredentials();
    }

    @Override
    public StorageCredentials credentialsForBucket(String uid, String bucket) {
        return new EmptyCredentials();
    }
}
