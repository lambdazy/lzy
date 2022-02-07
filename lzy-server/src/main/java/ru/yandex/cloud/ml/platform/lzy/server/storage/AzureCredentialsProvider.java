package ru.yandex.cloud.ml.platform.lzy.server.storage;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.Session;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AzureSASCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils;

import static ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils.createBucketIfNotExists;
import static ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils.getCredentialsByBucket;

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
