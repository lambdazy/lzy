package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import yandex.cloud.sdk.auth.provider.CredentialProvider;

@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
@Singleton
public class KuberClientFactoryImpl implements KuberClientFactory {

    private final CredentialProvider credentialProvider;

    @Inject
    public KuberClientFactoryImpl(CredentialProvider credentialProvider) {
        this.credentialProvider = credentialProvider;
    }

    @Override
    public KubernetesClient build(ClusterRegistry.ClusterDescription credentials) {
        final var config = new ConfigBuilder()
            .withMasterUrl(credentials.masterAddress())
            .withCaCertData(credentials.masterCert())
            .withOauthTokenProvider(() -> credentialProvider.get().getToken())
            .withRequestRetryBackoffInterval(/* millis */ 500)
            .withRequestRetryBackoffLimit(10)
            .build();
        return new KubernetesClientBuilder()
            .withConfig(config)
            .build();
    }
}
