package ru.yandex.cloud.ml.platform.lzy.server.storage;

import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;
import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AzureCredentials;

public class AzureCredentialsImpl extends AzureCredentials {

    private final String connectionString;

    public AzureCredentialsImpl(String connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public String connectionString() {
        return connectionString;
    }
}
