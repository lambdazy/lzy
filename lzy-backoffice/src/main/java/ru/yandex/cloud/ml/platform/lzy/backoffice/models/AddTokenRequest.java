package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class AddTokenRequest {
    private UserCredentials userCredentials;
    private String token;
    private String tokenName;

    public String getTokenName() {
        return tokenName;
    }

    public void setTokenName(String tokenName) {
        this.tokenName = tokenName;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public UserCredentials getUserCredentials() {
        return userCredentials;
    }

    public void setUserCredentials(UserCredentials userCredentials) {
        this.userCredentials = userCredentials;
    }

    public BackOffice.AddTokenRequest getModel(IAM.UserCredentials credentials){
        return BackOffice.AddTokenRequest.newBuilder()
                .setPublicKey(token)
                .setUserCredentials(userCredentials.getModel())
                .setBackofficeCredentials(credentials)
                .setTokenName(tokenName)
                .build();
    }
}
