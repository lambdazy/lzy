package ru.yandex.cloud.ml.platform.lzy.server.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Locale;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.storage.AmazonCredentialsImpl;
import ru.yandex.cloud.ml.platform.lzy.server.storage.StorageCredentialsProvider;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;

@Singleton
@Requires(property = "storage.amazon.enabled", value = "true")
@Requires(property = "database.enabled", value = "false", defaultValue = "false")
public class InMemAmazonCredentialsProvider implements StorageCredentialsProvider {
    @Inject
    StorageConfigs storageConfigs;

    @Override
    public StorageCredentials storageCredentials(String uid, String bucket) {
        return new AmazonCredentialsImpl(
            bucket,
            storageConfigs.getAmazon().getEndpoint(),
            storageConfigs.getAmazon().getAccessToken(),
            storageConfigs.getAmazon().getSecretToken()
        );
    }

    @Override
    public StorageCredentials separatedStorageCredentials(String uid, String bucket) {
        throw new NotImplementedException("Cannot implement separated storage without db");
    }
}
