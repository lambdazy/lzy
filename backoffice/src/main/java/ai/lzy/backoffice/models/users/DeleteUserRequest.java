package ai.lzy.backoffice.models.users;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.priv.v2.BackOffice;
import ai.lzy.priv.v2.IAM;

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
