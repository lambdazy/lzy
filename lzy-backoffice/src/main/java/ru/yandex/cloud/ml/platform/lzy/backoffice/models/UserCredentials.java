package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

@Introspected
public class UserCredentials {
    private String userId;
    private String sessionId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public BackOffice.BackofficeUserCredentials toModel(){
        return BackOffice.BackofficeUserCredentials.newBuilder()
                .setUserId(userId)
                .setSessionId(sessionId)
                .build();
    }
    public static UserCredentials fromModel(BackOffice.BackofficeUserCredentials model){
        UserCredentials creds = new UserCredentials();
        creds.setUserId(model.getUserId());
        creds.setSessionId(model.getSessionId());
        return creds;
    }
}
