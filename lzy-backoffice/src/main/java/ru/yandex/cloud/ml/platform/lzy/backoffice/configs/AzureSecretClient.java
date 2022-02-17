package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.StringReader;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;


@Requires(property = "azure-providers", value = "true")
@Singleton
public class AzureSecretClient implements CredentialsProvider {

    private final SecretClient secretClient;

    AzureSecretClient() {
        DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder()
            .build();

        secretClient = new SecretClientBuilder()
            .vaultUrl("https://lzy.vault.azure.net")
            .credential(defaultCredential)
            .buildClient();
    }

    public SecretClient getSecretClient() {
        return secretClient;
    }

    public IAM.UserCredentials createCreds() {
        String token;

        try (StringReader reader = new StringReader(
            secretClient.getSecret("backofficeKey").getValue())) {
            token = JwtCredentials.buildJWT(secretClient.getSecret("backofficeUserId").getValue(), reader);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            e.printStackTrace();
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "Corrupted backoffice token");
        }

        return IAM.UserCredentials.newBuilder()
            .setToken(token)
            .setUserId(secretClient.getSecret("backofficeUserId").getValue())
            .build();
    }
}
