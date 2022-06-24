package ai.lzy.backoffice.models.auth;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class AuthCodeRequest {

    private String code;
    private String state;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
