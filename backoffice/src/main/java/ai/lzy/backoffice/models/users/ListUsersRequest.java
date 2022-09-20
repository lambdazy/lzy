package ai.lzy.backoffice.models.users;

import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;
import io.micronaut.core.annotation.Introspected;

@Introspected
public class ListUsersRequest {

    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public BackOffice.ListUsersRequest toModel(LzyAuth.UserCredentials credentials) {
        return BackOffice.ListUsersRequest.newBuilder()
            .setCallerCredentials(this.credentials.toModel())
            .setBackofficeCredentials(credentials)
            .build();
    }
}
