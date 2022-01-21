package ru.yandex.cloud.ml.platform.lzy.server.storage;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

@Singleton
@Requires(property = "storage.amazon.enabled", value = "false", defaultValue = "false")
@Requires(property = "storage.azure.enabled", value = "false", defaultValue = "false")
public class SimpleStorageCredentialsProvider implements StorageCredentialsProvider {
    @Override
    public Lzy.GetS3CredentialsResponse storageCredentials(String uid) {
        return Lzy.GetS3CredentialsResponse.newBuilder().build();
    }

    @Override
    public Lzy.GetS3CredentialsResponse separatedStorageCredentials(String uid) {
        return Lzy.GetS3CredentialsResponse.newBuilder().build();
    }
}
