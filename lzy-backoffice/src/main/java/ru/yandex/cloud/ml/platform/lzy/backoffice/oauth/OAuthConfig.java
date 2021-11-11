package ru.yandex.cloud.ml.platform.lzy.backoffice.oauth;

import io.micronaut.context.annotation.ConfigurationProperties;

import javax.validation.constraints.NotBlank;

@ConfigurationProperties("oauth")
public class OAuthConfig {

    private GithubOAuthConfig github = new GithubOAuthConfig();

    public GithubOAuthConfig getGithub() {
        return github;
    }

    public void setGithub(GithubOAuthConfig github) {
        this.github = github;
    }

    @ConfigurationProperties("github")
    public static class GithubOAuthConfig{

        @NotBlank
        private String clientId;

        @NotBlank
        private String clientSecret;

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public String getClientSecret() {
            return clientSecret;
        }

        public void setClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
        }
    }
}
