package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.NoSuchElementException;

@Singleton
public class KuberNodeController implements NodeController {
    private static final Logger LOG = LogManager.getLogger(KuberNodeController.class);

    private final ClusterRegistry clusterRegistry;
    private final KuberClientFactory factory;
    private final Retry retry;

    public KuberNodeController(ClusterRegistry clusterRegistry, KuberClientFactory factory) {
        this.clusterRegistry = clusterRegistry;
        this.factory = factory;
        var retryConfig = new RetryConfig.Builder<>()
            .maxAttempts(3)
            .intervalFunction(IntervalFunction.ofExponentialBackoff(1000))
            .retryOnException(e -> e instanceof KubernetesClientException ke && !KuberUtils.isNotRetryable(ke))
            .build();
        retry = Retry.of("k8s-client-retry", retryConfig);
    }

    @Override
    public void addLabels(String clusterId, String nodeName, Map<String, String> labels) {
        LOG.debug("Adding labels; clusterId: {}, nodeName: {}, labels: {}", clusterId, nodeName, labels);
        final ClusterRegistry.ClusterDescription cluster;
        try {
            cluster = clusterRegistry.getCluster(clusterId);
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("Cluster not found");
        }

        try (final var client = factory.build(cluster)) {
            final Node node = client.nodes().withName(nodeName).get();
            if (node == null) {
                throw new IllegalArgumentException("Node not found");
            }
            retry.executeSupplier(() -> client.nodes().withName(nodeName).edit(n ->
                new NodeBuilder(n).editMetadata().addToLabels(labels).endMetadata().build()));
        }

        LOG.debug("Adding labels done; clusterId: {}, nodeName: {}, labels: {}", clusterId, nodeName, labels);
    }
}
