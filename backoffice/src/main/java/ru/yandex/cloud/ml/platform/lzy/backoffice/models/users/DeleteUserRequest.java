package ru.yandex.cloud.ml.platform.lzy.backoffice.models.users;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.models.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class DeleteUserRequest {

    private String userId;
    private UserCredentials deleterCredentials;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public UserCredentials getDeleterCredentials() {
        return deleterCredentials;
    }

    public void setDeleterCredentials(UserCredentials deleterCredentials) {
        this.deleterCredentials = deleterCredentials;
    }

    public BackOffice.DeleteUserRequest toModel(IAM.UserCredentials credentials) {
        return BackOffice.DeleteUserRequest.newBuilder()
            .setUserId(userId)
            .setBackofficeCredentials(credentials)
            .setDeleterCredentials(deleterCredentials.toModel())
            .build();
    }
}
