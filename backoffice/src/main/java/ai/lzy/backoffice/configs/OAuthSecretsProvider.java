package ai.lzy.backoffice.configs;

public interface OAuthSecretsProvider {

    GithubOAuthSecretsProvider getGithub();

    interface GithubOAuthSecretsProvider {

        String getClientId();

        String getClientSecret();
    }
}
