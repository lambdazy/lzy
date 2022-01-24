package ru.yandex.cloud.ml.platform.lzy.model;

public interface StorageCredentials {
    interface AzureCredentials {
        String connectionString();
    }

    interface AmazonCredentials {
        String endpoint();
        String accessToken();
        String secretToken();
    }

    interface AzureSASCredentials {
        String signature();
        String endpoint();
    }

    enum Type{
        Azure,
        AzureSas,
        Amazon,
        Empty
    }

    AzureCredentials azure();
    AmazonCredentials amazon();
    AzureSASCredentials azureSAS();
    Type type();
}
