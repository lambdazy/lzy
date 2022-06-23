package ai.lzy.server.storage;

import ai.lzy.model.StorageCredentials.AmazonCredentials;

public class AmazonCredentialsImpl extends AmazonCredentials {

    private final String endpoint;
    private final String accessToken;
    private final String secretToken;

    public AmazonCredentialsImpl(String endpoint, String accessToken,
                                 String secretToken) {
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
