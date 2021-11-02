package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class AddTokenRequest {
    private String userId;
    private String token;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public BackOffice.AddTokenRequest getModel(IAM.UserCredentials credentials){
        return BackOffice.AddTokenRequest.newBuilder()
                .setPublicKey(token)
                .setUserId(userId)
                .setBackofficeCredentials(credentials)
                .build();

    }
}
