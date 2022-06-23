package ai.lzy.backoffice.models.tasks;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.priv.v2.BackOffice;
import ai.lzy.priv.v2.IAM;

@Introspected
public class GetTasksRequest {

    private UserCredentials credentials;

    public UserCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(UserCredentials credentials) {
        this.credentials = credentials;
    }

    public BackOffice.GetTasksRequest toModel(IAM.UserCredentials credentials) {
        return BackOffice.GetTasksRequest.newBuilder()
            .setCredentials(this.credentials.toModel())
            .setBackofficeCredentials(credentials)
            .build();
    }
}
