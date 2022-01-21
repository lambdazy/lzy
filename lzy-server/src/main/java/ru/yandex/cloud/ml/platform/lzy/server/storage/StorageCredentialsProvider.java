package ru.yandex.cloud.ml.platform.lzy.server.storage;

import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

public interface StorageCredentialsProvider {
    Lzy.GetS3CredentialsResponse storageCredentials(String uid);
    Lzy.GetS3CredentialsResponse separatedStorageCredentials(String uid);
}
