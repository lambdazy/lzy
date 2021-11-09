package ru.yandex.cloud.ml.platform.lzy.backoffice.oauth.models;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class GithubAccessTokenRequest {
    private String client_id;
    private String client_secret;
    private String code;

    public String getClient_id() {
        return client_id;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public String getClient_secret() {
        return client_secret;
    }

    public void setClient_secret(String client_secret) {
        this.client_secret = client_secret;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
