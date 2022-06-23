package ai.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import ai.lzy.priv.v2.BackOffice;

@Introspected
public class UserCredentials {

    private String userId;
    private String sessionId;

    public static UserCredentials fromModel(BackOffice.BackofficeUserCredentials model) {
        UserCredentials creds = new UserCredentials();
        creds.setUserId(model.getUserId());
        creds.setSessionId(model.getSessionId());
        return creds;
    }

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

    public BackOffice.BackofficeUserCredentials toModel() {
        return BackOffice.BackofficeUserCredentials.newBuilder()
            .setUserId(userId)
            .setSessionId(sessionId)
            .build();
    }
}
