package ru.yandex.cloud.ml.platform.lzy.server.storage;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.hibernate.Session;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.DbStorage;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import static ru.yandex.cloud.ml.platform.lzy.server.utils.StorageUtils.getCredentialsByBucket;

@Singleton
@Requires(property = "database.enabled", value = "true", defaultValue = "false")
@Requires(property = "storage.azure.enabled", value = "true")
public class DbAzureCredentialsProvider implements StorageCredentialsProvider {
    @Inject
    StorageConfigs storageConfigs;

    @Inject
    DbStorage storage;


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
        try (Session session = storage.getSessionFactory().openSession()) {
            UserModel user = session.find(UserModel.class, uid);
            return Lzy.GetS3CredentialsResponse.newBuilder()
                    .setBucket(user.getBucket())
                    .setAzureSas(getCredentialsByBucket(uid, user.getBucket(), storageConfigs.getAzure()))
                    .build();
        }
    }
}
