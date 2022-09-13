package ai.lzy.site.models.auth;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class LoginResponse {

    private String redirectUrl;

    public String getRedirectUrl() {
        return redirectUrl;
    }

    public void setRedirectUrl(String redirectUrl) {
        this.redirectUrl = redirectUrl;
    }
}
