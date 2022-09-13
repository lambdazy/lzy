package ai.lzy.site.models.tasks;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.site.models.UserCredentials;
import ai.lzy.v1.BackOffice;
import ai.lzy.v1.IAM;

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
