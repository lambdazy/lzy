package ai.lzy.site.configs;

public interface OAuthSecretsProvider {

    GithubOAuthSecretsProvider getGithub();

    interface GithubOAuthSecretsProvider {

        String getClientId();

        String getClientSecret();
    }
}
