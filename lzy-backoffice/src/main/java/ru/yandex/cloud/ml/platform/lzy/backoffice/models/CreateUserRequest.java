package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

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

    public BackOffice.CreateUserRequest toModel(IAM.UserCredentials credentials){
        return BackOffice.CreateUserRequest.newBuilder()
                .setBackofficeCredentials(credentials)
                .setCreatorCredentials(creatorCredentials.toModel())
                .setUser(user.toModel())
                .build();
    }
}
