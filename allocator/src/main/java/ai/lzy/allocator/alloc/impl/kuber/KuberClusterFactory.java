package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Factory to build fabric8 kuber client
 * Implemented in separated interface for testing purposes
 */
public interface KuberClusterFactory {
    KubernetesClient build(ClusterRegistry.ClusterDescription credentials);
}
