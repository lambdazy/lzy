package ai.lzy.backoffice.models.users;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.priv.v2.BackOffice;

@Introspected
public class User {

    private String userId;

    public static User fromModel(BackOffice.User user) {
        User userModel = new User();
        userModel.setUserId(user.getUserId());
        return userModel;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public BackOffice.User toModel() {
        return BackOffice.User.newBuilder()
            .setUserId(userId)
            .build();
    }
}
