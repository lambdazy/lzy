package ai.lzy.site.models.users;

import ai.lzy.v1.deprecated.BackOffice;
import io.micronaut.core.annotation.Introspected;

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
