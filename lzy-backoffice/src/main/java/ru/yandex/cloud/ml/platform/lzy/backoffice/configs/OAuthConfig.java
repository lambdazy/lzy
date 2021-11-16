package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;

import javax.validation.constraints.NotBlank;

@Requires(property = "azure-providers", value = "false", defaultValue = "false")
@ConfigurationProperties("oauth")
public class OAuthConfig implements OAuthSecretsProvider{

    private GithubOAuthConfig github = new GithubOAuthConfig();

    public GithubOAuthConfig getGithub() {
        return github;
    }

    public void setGithub(GithubOAuthConfig github) {
        this.github = github;
    }

    @ConfigurationProperties("github")
    public static class GithubOAuthConfig implements OAuthSecretsProvider.GithubOAuthSecretsProvider{

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
