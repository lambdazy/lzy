package ai.lzy.backoffice.models.users;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.backoffice.models.UserCredentials;
import ai.lzy.v1.BackOffice;
import ai.lzy.v1.IAM;

@Introspected
public class CreateUserRequest {

    private UserCredentials creatorCredentials;
    private User user;

    public UserCredentials getCreatorCredentials() {
        return creatorCredentials;
    }

    public void setCreatorCredentials(UserCredentials creatorCredentials) {
        this.creatorCredentials = creatorCredentials;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public BackOffice.CreateUserRequest toModel(IAM.UserCredentials credentials) {
        return BackOffice.CreateUserRequest.newBuilder()
            .setBackofficeCredentials(credentials)
            .setCreatorCredentials(creatorCredentials.toModel())
            .setUser(user.toModel())
            .build();
    }
}
