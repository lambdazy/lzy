package ai.lzy.server.storage;

import ai.lzy.model.StorageCredentials.AzureCredentials;

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
