package ru.yandex.cloud.ml.platform.lzy.server.storage;

import static ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils.createBucketIfNotExists;
import static ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils.getCredentialsByBucket;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils;

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
        createBucketIfNotExists(storageConfigs.credentials(), bucket);
        return getCredentialsByBucket(uid, bucket, storageConfigs.getAzure());
    }
}
