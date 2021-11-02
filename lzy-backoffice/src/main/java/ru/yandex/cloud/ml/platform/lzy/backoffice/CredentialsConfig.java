package ru.yandex.cloud.ml.platform.lzy.backoffice;

import io.micronaut.context.annotation.ConfigurationProperties;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Credentials;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.UUID;

@ConfigurationProperties("credentials")
public class CredentialsConfig {
    private String userId;
    private String privateKey;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = privateKey;
    }

    public IAM.UserCredentials getCredentials() throws InvalidKeySpecException, NoSuchAlgorithmException, SignatureException, InvalidKeyException {
        UUID uuid = UUID.randomUUID();
        String token = uuid.toString() + "." + Credentials.signToken(uuid, privateKey);

        return IAM.UserCredentials.newBuilder()
                .setToken(token)
                .setUserId(userId)
                .build();
    }
}
