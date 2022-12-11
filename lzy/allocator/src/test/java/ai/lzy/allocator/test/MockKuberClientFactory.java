package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.vmpool.ClusterRegistry.ClusterDescription;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.function.Supplier;


@Requires(property = "allocator.mock-mk8s.enabled", value = "true")
@Singleton
public class MockKuberClientFactory implements KuberClientFactory {

    private Supplier<KubernetesClient> clientSupplier;

    public void setClientSupplier(Supplier<KubernetesClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    @Override
    public KubernetesClient build(ClusterDescription credentials) {
        return clientSupplier.get();
    }
}
