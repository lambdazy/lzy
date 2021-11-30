package ru.yandex.cloud.ml.platform.lzy.backoffice.models.keys;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class DeletePublicKeyRequest {
    UserCredentials credentials;
    private String keyName;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public BackOffice.DeleteTokenRequest toModel(IAM.UserCredentials credentials){
        return BackOffice.DeleteTokenRequest.newBuilder()
            .setBackofficeCredentials(credentials)
            .setCredentials(this.credentials.toModel())
            .setTokenName(this.keyName)
            .build();
    }
}
