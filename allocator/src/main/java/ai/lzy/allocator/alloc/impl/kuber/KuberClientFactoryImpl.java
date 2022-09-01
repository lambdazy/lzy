package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Requires(property = "allocator.yc-mk8s.enabled", value = "true")
@Singleton
public class KuberClientFactoryImpl implements KuberClientFactory {
    @Override
    public KubernetesClient build(ClusterRegistry.ClusterDescription credentials) {
        final var config = new ConfigBuilder()
            .withMasterUrl(credentials.masterAddress().toString())
            .withCaCertData(credentials.masterCert())
            .build();
        return new KubernetesClientBuilder()
            .withConfig(config)
            .build();
    }
}
