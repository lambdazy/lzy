package ai.lzy.backoffice.models.tasks;

import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;
import io.micronaut.core.annotation.Introspected;

@Introspected
public class GetTasksRequest {

    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public BackOffice.GetTasksRequest toModel(LzyAuth.UserCredentials credentials) {
        return BackOffice.GetTasksRequest.newBuilder()
            .setCredentials(this.credentials.toModel())
            .setBackofficeCredentials(credentials)
            .build();
    }
}
