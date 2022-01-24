package ru.yandex.cloud.ml.platform.lzy.server.storage;

import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials;

public class StorageCredentialsImpl implements StorageCredentials {

    private final Type type;
    private final AzureCredentials azure;
    private final AzureSASCredentials azureSAS;
    private final AmazonCredentials amazon;
    private final String bucket;

    public StorageCredentialsImpl(
        Type type,
        AzureCredentials azure,
        AzureSASCredentials azureSAS,
        AmazonCredentials amazon,
        String bucket
    ) {
        this.type = type;
        this.azure = azure;
        this.azureSAS = azureSAS;
        this.amazon = amazon;
        this.bucket = bucket;
    }

    public static class AzureCredentialsImpl implements AzureCredentials{

        private final String connectionString;

        public AzureCredentialsImpl(String connectionString) {
            this.connectionString = connectionString;
        }

        @Override
        public String connectionString() {
            return connectionString;
        }
    }

    public static class AzureSASCredentialsImpl implements AzureSASCredentials{

        private final String signature;

        private final String endpoint;

        public AzureSASCredentialsImpl(String signature, String endpoint) {
            this.signature = signature;
            this.endpoint = endpoint;
        }

        @Override
        public String signature() {
            return signature;
        }

        @Override
        public String endpoint() {
            return endpoint;
        }
    }

    public static class AmazonCredentialsImpl implements AmazonCredentials{

        private final String endpoint;
        private final String accessToken;
        private final String secretToken;

        public AmazonCredentialsImpl(String endpoint, String accessToken, String secretToken) {
            this.endpoint = endpoint;
            this.accessToken = accessToken;
            this.secretToken = secretToken;
        }

        @Override
        public String endpoint() {
            return endpoint;
        }

        @Override
        public String accessToken() {
            return accessToken;
        }

        @Override
        public String secretToken() {
            return secretToken;
        }
    }

    @Override
    public AzureCredentials azure() {
        return azure;
    }

    @Override
    public AmazonCredentials amazon() {
        return amazon;
    }

    @Override
    public AzureSASCredentials azureSAS() {
        return azureSAS;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String bucket() {
        return bucket;
    }

    public static StorageCredentials azure(String connectionString, String bucket){
        return new StorageCredentialsImpl(
            Type.Azure,
            new AzureCredentialsImpl(connectionString),
            null, null, bucket
        );
    }

    public static StorageCredentials azureSAS(String signature, String endpoint, String bucket){
        return new StorageCredentialsImpl(
            Type.AzureSas,
            null,
            new AzureSASCredentialsImpl(signature, endpoint),
            null, bucket
        );
    }

    public static StorageCredentials amazon(String endpoint, String accessToken, String secretToken, String bucket){
        return new StorageCredentialsImpl(
            Type.Amazon,
            null,
            null,
            new AmazonCredentialsImpl(endpoint, accessToken, secretToken),
            bucket
        );
    }

}
