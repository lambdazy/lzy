package ai.lzy.server.mem;

import ai.lzy.server.configs.StorageConfigs;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ai.lzy.server.storage.StorageCredentialsProvider;
import ai.lzy.server.utils.azure.StorageUtils;

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
