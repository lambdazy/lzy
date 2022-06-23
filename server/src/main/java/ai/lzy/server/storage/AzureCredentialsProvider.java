package ai.lzy.server.storage;

import ai.lzy.server.configs.StorageConfigs;
import ai.lzy.server.utils.azure.StorageUtils;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ai.lzy.model.StorageCredentials;

@Singleton
@Requires(property = "storage.azure.enabled", value = "true")
public class AzureCredentialsProvider implements StorageCredentialsProvider {
    @Inject
    StorageConfigs storageConfigs;


    @Override
    public StorageCredentials storageCredentials() {
        StorageUtils.createBucketIfNotExists(storageConfigs.credentials(), storageConfigs.getBucket());
        return storageConfigs.credentials();
    }

    @Override
    public StorageCredentials credentialsForBucket(String uid, String bucket) {
        StorageUtils.createBucketIfNotExists(storageConfigs.credentials(), bucket);
        return StorageUtils.getCredentialsByBucket(uid, bucket, storageConfigs.getAzure());
    }
}
