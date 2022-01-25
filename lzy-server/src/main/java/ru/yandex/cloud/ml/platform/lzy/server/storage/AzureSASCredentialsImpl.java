package ru.yandex.cloud.ml.platform.lzy.server.storage;

import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AzureSASCredentials;

public class AzureSASCredentialsImpl extends AzureSASCredentials {

    private final String signature;
    private final String endpoint;


    public AzureSASCredentialsImpl(String bucket, String signature, String endpoint) {
        super(bucket);
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
