package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.exceptions.NetworkPolicyException;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.networking.v1.*;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static ai.lzy.allocator.alloc.impl.kuber.KuberVmAllocator.NAMESPACE_VALUE;
import static ai.lzy.allocator.util.KuberUtils.isNotRetryable;
import static ai.lzy.allocator.util.KuberUtils.isResourceAlreadyExist;

@Singleton
@Slf4j
@Requires(property = "allocator.policy.enabled", value = "true")
public class KuberNetworkPolicyManager implements NetworkPolicyManager {

    private static final String NAME_PREFIX = "np-for-";

    private final KuberClientFactory clientFactory;
    private final ClusterRegistry clusterRegistry;
    private final List<PolicyRule> whitelist;

    @Inject
    public KuberNetworkPolicyManager(KuberClientFactory clientFactory, ServiceConfig config,
                                     ClusterRegistry clusterRegistry)
    {
        this.clientFactory = clientFactory;
        this.clusterRegistry = clusterRegistry;
        this.whitelist = config.getServiceCidrs().stream().toList();
    }

    @Override
    public void createNetworkPolicy(String sessionId, List<PolicyRule> whitelistCIDRs) throws NetworkPolicyException {
        var clusterDescriptions = clusterRegistry.listClusters(ClusterRegistry.ClusterType.User);
        clusterDescriptions.forEach(cluster -> {
            try (final var client = clientFactory.build(cluster)) {
                LabelSelector sameSessionIdPodSelector = new LabelSelectorBuilder().withMatchLabels(
                        Map.of(KuberLabels.LZY_POD_SESSION_ID_LABEL, sessionId)
                    )
                    .build();
                var ingressRules = new ArrayList<NetworkPolicyIngressRule>();
                ingressRules.add(
                    new NetworkPolicyIngressRuleBuilder()
                        .withFrom(
                            new NetworkPolicyPeerBuilder()
                                .withPodSelector(sameSessionIdPodSelector)
                                .build()
                        )
                        .build()
                );
                ingressRules.addAll(whitelist.stream().map(this::toK8sPolicy).toList());
                ingressRules.addAll(whitelistCIDRs.stream().map(this::toK8sPolicy).toList());
                NetworkPolicy networkPolicySpec = new NetworkPolicyBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName(NAME_PREFIX + sessionId).build())
                    .withSpec(
                        new NetworkPolicySpecBuilder()
                            .withPodSelector(sameSessionIdPodSelector)
                            .withPolicyTypes(List.of("Ingress"))
                            .withIngress(ingressRules)
                            .build()
                    )
                    .build();
                final NetworkPolicy networkPolicy;
                try {
                    networkPolicy = client.network()
                        .networkPolicies()
                        .inNamespace(NAMESPACE_VALUE)
                        .resource(networkPolicySpec).create();
                } catch (Exception e) {
                    if (isResourceAlreadyExist(e)) {
                        log.warn("Session: {} already exists", sessionId);
                        return;
                    }
                    log.error("Failed to create network policy for session {}: {}", sessionId, e.getMessage(), e);
                    throw new NetworkPolicyException(
                        "Failed to create network policy for session " + sessionId + ": " + e.getMessage(), false
                    );
                }
                log.debug("Created network policy in Kuber: {}", networkPolicy);
            }
        });
    }

    private NetworkPolicyIngressRule toK8sPolicy(PolicyRule rule) {
        var builder = new NetworkPolicyIngressRuleBuilder()
            .withFrom(
                new NetworkPolicyPeerBuilder()
                    .withIpBlock(new IPBlockBuilder().withCidr(rule.cidr()).build())
                    .build()
            );
        if (!rule.ports().isEmpty()) {
            builder.withPorts(rule.ports().stream()
                .map(p -> new NetworkPolicyPortBuilder().withPort(new IntOrString(p)).build()).toList());
        }
        return builder.build();
    }

    @Override
    public void deleteNetworkPolicy(String sessionId) throws NetworkPolicyException {
        var clusterDescriptions = clusterRegistry.listClusters(ClusterRegistry.ClusterType.User);
        clusterDescriptions.forEach(cluster -> {
            try (final var client = clientFactory.build(cluster)) {
                NetworkPolicy networkPolicySpec = new NetworkPolicyBuilder()
                    .withMetadata(new ObjectMetaBuilder().withName(NAME_PREFIX + sessionId).build())
                    .build();
                final List<StatusDetails> deleteStatusDetails;
                try {
                    try {
                        var get = client.network()
                            .networkPolicies()
                            .inNamespace(NAMESPACE_VALUE)
                            .resource(networkPolicySpec)
                            .get();
                    } catch (KubernetesClientException e) {
                        log.info("NetPolicy {} not found", sessionId);
                        return;
                    }
                    deleteStatusDetails = client.network()
                        .networkPolicies()
                        .inNamespace(NAMESPACE_VALUE)
                        .resource(networkPolicySpec)
                        .delete();
                } catch (KubernetesClientException e) {
                    log.error("Failed to delete network policy for session {}: {}", sessionId, e.getMessage(), e);
                    if (isNotRetryable(e)) {
                        throw new NetworkPolicyException(e.getMessage(), false);
                    }
                    throw new NetworkPolicyException(e.getMessage(), true);
                }
                log.debug("Deleted network policy in Kuber: {}", deleteStatusDetails);
            }
        });
    }
}
