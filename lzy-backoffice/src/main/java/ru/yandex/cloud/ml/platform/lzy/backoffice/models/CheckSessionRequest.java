package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import ru.yandex.cloud.ml.platform.lzy.backoffice.CredentialsConfig;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

@Introspected
public class CheckSessionRequest {
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

    public BackOffice.CheckSessionRequest toModel(IAM.UserCredentials credentials){
        return BackOffice.CheckSessionRequest.newBuilder()
                .setBackofficeCredentials(credentials)
                .setSessionId(sessionId)
                .setUserId(userId)
                .build();
    }
}
