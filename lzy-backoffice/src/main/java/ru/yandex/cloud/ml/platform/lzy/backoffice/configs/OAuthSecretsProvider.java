package ru.yandex.cloud.ml.platform.lzy.backoffice.configs;

public interface OAuthSecretsProvider {
    GithubOAuthSecretsProvider getGithub();

    interface GithubOAuthSecretsProvider {
        String getClientId();
        String getClientSecret();
    }
}
