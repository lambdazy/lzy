package ru.yandex.cloud.ml.platform.lzy.server.storage;

import ru.yandex.cloud.ml.platform.lzy.model.StorageCredentials.AmazonCredentials;

public class AmazonCredentialsImpl extends AmazonCredentials {

    private final String endpoint;
    private final String accessToken;
    private final String secretToken;

    public AmazonCredentialsImpl(String bucket, String endpoint, String accessToken,
        String secretToken) {
        super(bucket);
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
