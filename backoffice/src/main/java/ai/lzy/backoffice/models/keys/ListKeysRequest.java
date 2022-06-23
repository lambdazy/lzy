package ai.lzy.backoffice.models.keys;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.priv.v2.BackOffice;
import ai.lzy.priv.v2.IAM;

@Introspected
public class ListKeysRequest {

    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public BackOffice.ListKeysRequest toModel(IAM.UserCredentials credentials) {
        return BackOffice.ListKeysRequest.newBuilder()
            .setCredentials(this.credentials.toModel())
            .setBackofficeCredentials(credentials)
            .build();
    }
}
