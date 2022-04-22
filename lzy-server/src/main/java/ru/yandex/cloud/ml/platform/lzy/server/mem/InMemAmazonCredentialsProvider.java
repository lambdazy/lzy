package ru.yandex.cloud.ml.platform.lzy.server.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.storage.AmazonCredentialsImpl;
import ru.yandex.cloud.ml.platform.lzy.server.storage.StorageCredentialsProvider;
import ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils;

@Singleton
@Requires(property = "storage.amazon.enabled", value = "true")
@Requires(property = "database.enabled", value = "false", defaultValue = "false")
public class InMemAmazonCredentialsProvider implements StorageCredentialsProvider {
    @Inject
    StorageConfigs storageConfigs;

    @Override
    public StorageCredentials storageCredentials() {
        StorageUtils.createBucketIfNotExists(storageConfigs.credentials(), storageConfigs.getBucket());
        return storageConfigs.credentials();
    }

    @Override
    public StorageCredentials credentialsForBucket(String uid, String bucket) {
        throw new NotImplementedException("Cannot implement separated storage without db");
    }
}
