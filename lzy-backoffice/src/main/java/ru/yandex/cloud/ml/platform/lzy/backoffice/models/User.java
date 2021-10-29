package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

@Introspected
public class User {
    private String userId;
    private String publicKey;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicToken) {
        this.publicKey = publicToken;
    }

    public BackOffice.User getModel(){
        return BackOffice.User.newBuilder()
                .setUserId(userId)
                .setPublicKey(publicKey)
                .build();
    }
}
