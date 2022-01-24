package ru.yandex.cloud.ml.platform.lzy.server.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AzureSASCredentials;
import ru.yandex.cloud.ml.platform.lzy.server.storage.AzureCredentialsImpl;
import ru.yandex.cloud.ml.platform.lzy.server.storage.AzureSASCredentialsImpl;
import ru.yandex.cloud.ml.platform.lzy.server.storage.StorageCredentialsProvider;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils;

import java.util.Locale;

@Singleton
@Requires(property = "storage.azure.enabled", value = "true")
@Requires(property = "database.enabled", value = "false", defaultValue = "false")
public class InMemAzureCredentialsProvider implements StorageCredentialsProvider {

    @Inject
    private StorageConfigs storageConfigs;

    @Override
    public StorageCredentials storageCredentials(String uid) {
        return new AzureCredentialsImpl(
            storageConfigs.getAzure().getConnectionString(),
            storageConfigs.getBucket()
        );
    }

    @Override
    public StorageCredentials separatedStorageCredentials(String uid) {
        AzureSASCredentials credentials = StorageUtils.getCredentialsByBucket(uid, uid.toLowerCase(Locale.ROOT), storageConfigs.getAzure());

        return new AzureSASCredentialsImpl(
            uid.toLowerCase(Locale.ROOT),
            credentials.signature(),
            credentials.endpoint()
        );
    }
}
