package ru.yandex.cloud.ml.platform.lzy.server.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.server.storage.StorageCredentialsProvider;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.utils.StorageUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.util.Locale;

@Singleton
@Requires(property = "storage.azure.enabled", value = "true")
@Requires(property = "database.enabled", value = "false", defaultValue = "false")
public class InMemAzureCredentialsProvider implements StorageCredentialsProvider {

    @Inject
    private StorageConfigs storageConfigs;

    @Override
    public Lzy.GetS3CredentialsResponse storageCredentials(String uid) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
                .setAzure(
                        Lzy.AzureCredentials.newBuilder()
                                .setConnectionString(storageConfigs.getAzure().getConnectionString())
                                .build()
                )
                .setBucket(storageConfigs.getBucket())
                .build();
    }

    @Override
    public Lzy.GetS3CredentialsResponse separatedStorageCredentials(String uid) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
                .setAzureSas(StorageUtils.getCredentialsByBucket(uid, uid.toLowerCase(Locale.ROOT), storageConfigs.getAzure()))
                .build();
    }
}
