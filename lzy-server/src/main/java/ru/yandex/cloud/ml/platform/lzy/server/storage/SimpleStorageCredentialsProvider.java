package ru.yandex.cloud.ml.platform.lzy.server.storage;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.EmptyCredentials;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.Type;

@Singleton
@Requires(property = "storage.amazon.enabled", value = "false", defaultValue = "false")
@Requires(property = "storage.azure.enabled", value = "false", defaultValue = "false")
public class SimpleStorageCredentialsProvider implements StorageCredentialsProvider {
    @Override
    public StorageCredentials storageCredentials(String uid) {
        return new EmptyCredentials();
    }

    @Override
    public StorageCredentials separatedStorageCredentials(String uid) {
        return new EmptyCredentials();
    }
}
