package ai.lzy.backoffice.models.users;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.priv.v2.BackOffice;
import ai.lzy.priv.v2.IAM;

@Introspected
public class ListUsersRequest {

    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public BackOffice.ListUsersRequest toModel(IAM.UserCredentials credentials) {
        return BackOffice.ListUsersRequest.newBuilder()
            .setCallerCredentials(this.credentials.toModel())
            .setBackofficeCredentials(credentials)
            .build();
    }
}
