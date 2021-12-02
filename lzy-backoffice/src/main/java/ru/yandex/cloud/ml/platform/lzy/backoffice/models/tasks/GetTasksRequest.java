package ru.yandex.cloud.ml.platform.lzy.backoffice.models.tasks;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

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
