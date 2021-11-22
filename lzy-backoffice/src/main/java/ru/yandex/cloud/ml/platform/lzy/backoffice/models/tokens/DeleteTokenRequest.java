package ru.yandex.cloud.ml.platform.lzy.backoffice.models.tokens;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class DeleteTokenRequest {
    UserCredentials credentials;
    private String tokenName;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public String getTokenName() {
        return tokenName;
    }

    public void setTokenName(String tokenName) {
        this.tokenName = tokenName;
    }

    public BackOffice.DeleteTokenRequest toModel(IAM.UserCredentials credentials){
        return BackOffice.DeleteTokenRequest.newBuilder()
            .setBackofficeCredentials(credentials)
            .setCredentials(this.credentials.toModel())
            .setTokenName(this.tokenName)
            .build();
    }
}
