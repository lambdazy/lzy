package ai.lzy.backoffice.configs;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Requires(property = "azure-providers", value = "true")
@Singleton
public class OAuthProviderAzure implements OAuthSecretsProvider {

    @Inject
    GithubOAuthProviderAzure github;

    @Override
    public GithubOAuthProviderAzure getGithub() {
        return github;
    }

    @Singleton
    public class GithubOAuthProviderAzure implements
        OAuthSecretsProvider.GithubOAuthSecretsProvider {

        @Inject
        AzureSecretClient secrets;

        @Override
        public String getClientId() {
            return secrets.getSecretClient().getSecret("githubClientId").getValue();
        }

        @Override
        public String getClientSecret() {
            return secrets.getSecretClient().getSecret("githubClientSecret").getValue();
        }
    }
}
