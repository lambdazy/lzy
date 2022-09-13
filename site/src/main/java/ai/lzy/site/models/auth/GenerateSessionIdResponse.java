package ai.lzy.site.models.auth;

import ai.lzy.v1.deprecated.BackOffice;
import io.micronaut.core.annotation.Introspected;

@Introspected
public class GenerateSessionIdResponse {

    String sessionId;

    public static GenerateSessionIdResponse fromModel(BackOffice.GenerateSessionIdResponse model) {
        GenerateSessionIdResponse resp = new GenerateSessionIdResponse();
        resp.setSessionId(model.getSessionId());
        return resp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
