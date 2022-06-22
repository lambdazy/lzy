package ru.yandex.cloud.ml.platform.lzy.backoffice.models.auth;

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
