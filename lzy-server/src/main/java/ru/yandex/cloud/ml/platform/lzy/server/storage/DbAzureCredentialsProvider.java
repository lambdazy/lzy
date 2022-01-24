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

import static ru.yandex.cloud.ml.platform.lzy.server.utils.azure.StorageUtils.getCredentialsByBucket;

@Singleton
@Requires(property = "database.enabled", value = "true")
@Requires(property = "storage.azure.enabled", value = "true")
public class DbAzureCredentialsProvider implements StorageCredentialsProvider {
    @Inject
    StorageConfigs storageConfigs;

    @Inject
    DbStorage storage;


    @Override
    public StorageCredentials storageCredentials(String uid) {
        return new AzureCredentialsImpl(
            storageConfigs.getAzure().getConnectionString(),
            storageConfigs.getBucket()
        );
    }

    @Override
    public StorageCredentials separatedStorageCredentials(String uid) {
        try (Session session = storage.getSessionFactory().openSession()) {
            UserModel user = session.find(UserModel.class, uid);
            AzureSASCredentials credentials = getCredentialsByBucket(uid, user.getBucket(), storageConfigs.getAzure());
            return new AzureSASCredentialsImpl(
                user.getBucket(),
                credentials.signature(),
                credentials.endpoint()
            );
        }
    }
}
