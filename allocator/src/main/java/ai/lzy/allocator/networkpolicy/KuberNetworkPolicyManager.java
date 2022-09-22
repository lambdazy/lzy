package ai.lzy.allocator.networkpolicy;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberLabels;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import com.google.common.net.HostAndPort;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.networking.v1.*;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

@Singleton
@Requires(property = "allocator.kuber-network-policy-manager.enabled", value = "true")
public class KuberNetworkPolicyManager implements NetworkPolicyManager {
    private static final Logger LOG = LogManager.getLogger(KuberNetworkPolicyManager.class);
    private static final String NAMESPACE = "default";

    private final KuberClientFactory factory;
    private final ClusterRegistry clusterRegistry;
    private final String allocatorV4Cidr;

    @Inject
    public KuberNetworkPolicyManager(ServiceConfig config, KuberClientFactory factory,
                                     ClusterRegistry poolRegistry)
    {
        allocatorV4Cidr = HostAndPort.fromString(config.getAddress()).getHost() + "/32";
        this.factory = factory;
        this.clusterRegistry = poolRegistry;
    }

    public void createNetworkPolicy(String sessionId) {
        var clusterDescriptions = clusterRegistry.listClusters(ClusterRegistry.ClusterType.User);
        clusterDescriptions.forEach(cluster -> {
            try (final var client = factory.build(cluster)) {
                LabelSelector sameSessionIdPodSelector = new LabelSelectorBuilder().withMatchLabels(
                        Map.of(KuberLabels.LZY_POD_SESSION_ID_LABEL, sessionId)
                    )
                    .build();
                NetworkPolicy networkPolicySpec = new NetworkPolicyBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName("pods-network-policy-" + sessionId).build())
                    .withSpec(
                        new NetworkPolicySpecBuilder()
                            .withPodSelector(sameSessionIdPodSelector)
                            .withPolicyTypes(List.of("Ingress", "Egress"))
                            .withIngress(
                                new NetworkPolicyIngressRuleBuilder()
                                    .withFrom(
                                        new NetworkPolicyPeerBuilder()
                                            .withPodSelector(sameSessionIdPodSelector)
                                            .build()
                                    )
                                    .build(),
                                new NetworkPolicyIngressRuleBuilder()
                                    .withFrom(
                                        new NetworkPolicyPeerBuilder()
                                            .withIpBlock(new IPBlockBuilder().withCidr(allocatorV4Cidr).build())
                                            .build()
                                    )
                                    .build()
                            )
                            .withEgress(
                                new NetworkPolicyEgressRuleBuilder()
                                    .withTo(
                                        new NetworkPolicyPeerBuilder()
                                            .withPodSelector(sameSessionIdPodSelector)
                                            .build()
                                    )
                                    .build(),
                                new NetworkPolicyEgressRuleBuilder()
                                    .withTo(
                                        new NetworkPolicyPeerBuilder()
                                            .withIpBlock(new IPBlockBuilder().withCidr(allocatorV4Cidr).build())
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build();
                final NetworkPolicy networkPolicy;
                try {
                    var resource = client.network()
                        .networkPolicies()
                        .inNamespace(NAMESPACE)
                        .resource(networkPolicySpec);
                    networkPolicy = resource.create();
                } catch (Exception e) {
                    LOG.error("Failed to create network policy for session {}: {}", sessionId, e.getMessage(), e);
                    throw new RuntimeException(
                        "Failed to create network policy for session " + sessionId + ": " + e.getMessage(), e
                    );
                }
                LOG.debug("Created network policy in Kuber: {}", networkPolicy);
            }
        });
    }

    public void deleteNetworkPolicy(String sessionId) {
        var clusterDescriptions = clusterRegistry.listClusters(ClusterRegistry.ClusterType.User);
        clusterDescriptions.forEach(cluster -> {
            try (final var client = factory.build(cluster)) {
                NetworkPolicy networkPolicySpec = new NetworkPolicyBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName("pods-network-policy-" + sessionId).build())
                    .build();
                final List<StatusDetails> deleteStatusDetails;
                try {
                    deleteStatusDetails = client.network()
                        .networkPolicies()
                        .inNamespace(NAMESPACE)
                        .resource(networkPolicySpec)
                        .delete();
                } catch (Exception e) {
                    LOG.error("Failed to delete network policy for session {}: {}", sessionId, e.getMessage(), e);
                    throw new RuntimeException(
                        "Failed to delete network policy for session " + sessionId + ": " + e.getMessage(), e
                    );
                }
                LOG.debug("Deleted network policy in Kuber: {}", deleteStatusDetails);
            }
        });
    }
}
