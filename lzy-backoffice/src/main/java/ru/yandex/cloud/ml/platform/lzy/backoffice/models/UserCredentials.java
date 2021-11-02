package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

@Introspected
public class UserCredentials {
    private String userId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public BackOffice.BackofficeUserCredentials getModel(){
        return BackOffice.BackofficeUserCredentials.newBuilder()
                .setUserId(userId)
                .build();
    }
}
