package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.vmpool.VmPoolRegistry;
import com.google.common.net.HostAndPort;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import jakarta.inject.Singleton;

@Singleton
public class KuberClusterFactoryImpl implements KuberClusterFactory {
    @Override
    public KubernetesClient build(VmPoolRegistry.ClusterCredentials credentials) {
        final var config = new ConfigBuilder()
            .withMasterUrl(credentials.masterAddress().toString())
            .withCaCertData(credentials.masterCert())
            .build();
        return new KubernetesClientBuilder()
            .withConfig(config)
            .build();
    }
}
