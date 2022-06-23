package ai.lzy.backoffice.models.auth;


import io.micronaut.core.annotation.Introspected;
import ai.lzy.priv.v2.BackOffice;

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
