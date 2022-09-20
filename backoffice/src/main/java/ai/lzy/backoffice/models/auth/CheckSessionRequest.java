package ai.lzy.backoffice.models.auth;

import ai.lzy.v1.deprecated.BackOffice;
import ai.lzy.v1.deprecated.LzyAuth;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.Nullable;

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

    public BackOffice.CheckSessionRequest toModel(LzyAuth.UserCredentials credentials) {
        BackOffice.CheckSessionRequest.Builder builder = BackOffice.CheckSessionRequest.newBuilder()
            .setBackofficeCredentials(credentials)
            .setSessionId(sessionId);
        if (userId != null) {
            builder.setUserId(userId);
        }
        return builder.build();
    }
}
