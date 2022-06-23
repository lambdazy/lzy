package ai.lzy.backoffice.models.auth;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.Nullable;
import ai.lzy.priv.v2.BackOffice;
import ai.lzy.priv.v2.IAM;

@Introspected
public class CheckSessionRequest {

    private String userId;

    @Nullable
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

    public BackOffice.CheckSessionRequest toModel(IAM.UserCredentials credentials) {
        BackOffice.CheckSessionRequest.Builder builder = BackOffice.CheckSessionRequest.newBuilder()
            .setBackofficeCredentials(credentials)
            .setSessionId(sessionId);
        if (userId != null) {
            builder.setUserId(userId);
        }
        return builder.build();
    }
}
