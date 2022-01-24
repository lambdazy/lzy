package ru.yandex.cloud.ml.platform.lzy.server.storage;

import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;

public interface StorageCredentialsProvider {
    StorageCredentials storageCredentials(String uid);
    StorageCredentials separatedStorageCredentials(String uid);
}
