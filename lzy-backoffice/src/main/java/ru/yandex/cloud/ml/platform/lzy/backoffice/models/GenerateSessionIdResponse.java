package ru.yandex.cloud.ml.platform.lzy.backoffice.models;


import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

@Introspected
public class GenerateSessionIdResponse {
    String sessionId;

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public static GenerateSessionIdResponse fromModel(BackOffice.GenerateSessionIdResponse model){
        GenerateSessionIdResponse resp = new GenerateSessionIdResponse();
        resp.setSessionId(model.getSessionId());
        return resp;
    }
}
