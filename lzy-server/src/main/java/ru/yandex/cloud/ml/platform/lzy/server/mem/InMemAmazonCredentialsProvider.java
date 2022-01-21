package ru.yandex.cloud.ml.platform.lzy.server.mem;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.cloud.ml.platform.lzy.server.storage.StorageCredentialsProvider;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

@Singleton
@Requires(property = "storage.amazon.enabled", value = "true")
@Requires(property = "database.enabled", value = "false", defaultValue = "false")
public class InMemAmazonCredentialsProvider implements StorageCredentialsProvider {
    @Inject
    StorageConfigs storageConfigs;

    @Override
    public Lzy.GetS3CredentialsResponse storageCredentials(String uid) {
        return Lzy.GetS3CredentialsResponse.newBuilder()
            .setBucket(storageConfigs.getBucket())
            .setAmazon(
                Lzy.AmazonCredentials.newBuilder()
                    .setAccessToken(storageConfigs.getAmazon().getAccessToken())
                    .setSecretToken(storageConfigs.getAmazon().getSecretToken())
                    .setEndpoint(storageConfigs.getAmazon().getEndpoint())
                    .build()
            )
            .build();
    }

    @Override
    public Lzy.GetS3CredentialsResponse separatedStorageCredentials(String uid) {
        throw new NotImplementedException("Cannot implement separated storage without db");
    }
}
