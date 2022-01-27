package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import java.io.FileReader;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.UUID;
import ru.yandex.cloud.ml.platform.lzy.model.utils.Credentials;
import ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Requires(property = "azure-providers", value = "false", defaultValue = "false")
@ConfigurationProperties("credentials")
public class CredentialsConfig implements CredentialsProvider {

    private String userId;
    private String privateKeyPath;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPrivateKeyPath() {
        return privateKeyPath;
    }

    public void setPrivateKeyPath(String privateKeyPath) {
        this.privateKeyPath = privateKeyPath;
    }

    public IAM.UserCredentials createCreds() {
        String token;
        try (FileReader keyReader = new FileReader(privateKeyPath)) {
            token = JwtCredentials.buildJWT(userId, keyReader);
        } catch (InvalidKeySpecException | NoSuchAlgorithmException | IOException e) {
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "Corrupted backoffice token");
        }

        return IAM.UserCredentials.newBuilder()
            .setToken(token)
            .setUserId(userId)
            .build();
    }
}
