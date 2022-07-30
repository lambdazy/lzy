package ai.lzy.server.storage;

import ai.lzy.model.StorageCredentials;

public interface StorageCredentialsProvider {
    StorageCredentials storageCredentials();

    StorageCredentials credentialsForBucket(String uid, String bucket);
}
