package ru.yandex.cloud.ml.platform.lzy.backoffice.models.keys;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class ListKeysRequest {
    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public BackOffice.ListTokensRequest toModel(IAM.UserCredentials credentials){
        return BackOffice.ListTokensRequest.newBuilder()
            .setCredentials(this.credentials.toModel())
            .setBackofficeCredentials(credentials)
            .build();
    }
}
