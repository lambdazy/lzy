package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.vmpool.ClusterRegistry.ClusterDescription;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Primary;
import jakarta.inject.Singleton;


@Primary
@Singleton
public class MockKuberClientFactory implements KuberClientFactory {

    private KubernetesClient client;

    public void setClient(KubernetesClient client) {
        this.client = client;
    }

    @Override
    public KubernetesClient build(ClusterDescription credentials) {
        return client;
    }
}
