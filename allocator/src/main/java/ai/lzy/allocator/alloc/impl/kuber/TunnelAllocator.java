package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodAffinityTermBuilder;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

@Singleton
public class TunnelAllocator {
    private static final Logger LOG = LogManager.getLogger(TunnelAllocator.class);
    private static final String NAMESPACE = "default";
    public static final String TUNNEL_POD_NAME_PREFIX = "lzy-tunnel-";

    private final ClusterRegistry clusterRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;

    public TunnelAllocator(ClusterRegistry clusterRegistry, KuberClientFactory factory, ServiceConfig config) {
        this.clusterRegistry = clusterRegistry;
        this.factory = factory;
        this.config = config;
    }

    public void allocateTunnel(Vm.Spec vmSpec) throws InvalidConfigurationException {
        final var cluster = clusterRegistry.findCluster(
            vmSpec.poolLabel(), vmSpec.zone(), ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vmSpec.poolLabel() + " and zone " + vmSpec.zone());
        }

        try (final var client = factory.build(cluster)) {
            Pod tunnelPod = new PodSpecBuilder(
                vmSpec, client, config, PodSpecBuilder.TUNNEL_POD_TEMPLATE_PATH, TUNNEL_POD_NAME_PREFIX
            ).withWorkloads(
                Collections.singletonList(
                    new Workload(
                        "tunnel", config.getTunnelPodImage(), Collections.emptyMap(),
                        Collections.emptyList(), Collections.emptyMap(), Collections.emptyList(),
                        false
                    )
                )
            ).build();

            tunnelPod.getSpec()
                .getAffinity()
                .getPodAntiAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution()
                .add(
                    new PodAffinityTermBuilder()
                        .withLabelSelector(
                            new LabelSelectorBuilder()
                                .withMatchExpressions(
                                    new LabelSelectorRequirementBuilder()
                                        .withKey(KuberLabels.LZY_POD_SESSION_ID_LABEL)
                                        .withOperator("NotIn")
                                        .withValues(vmSpec.sessionId())
                                        .build()
                                ).build()
                        ).build()
                );

            try {
                tunnelPod = client.pods()
                    .inNamespace(NAMESPACE)
                    .resource(tunnelPod)
                    .create();
            } catch (Exception e) {
                LOG.error("Failed to allocate tunnel pod: {}", e.getMessage(), e);
                //TODO (tomato): add retries here if the error is caused due to temporal problems with kuber
                throw new RuntimeException("Failed to allocate tunnel pod: " + e.getMessage(), e);
            }
            LOG.debug("Created tunnel pod in Kuber: {}", tunnelPod);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Database error: " + e.getMessage(), e);
        }
    }

    public Workload createRequestTunnelInitContainer(Vm.Spec vmSpec, String remoteV6)
        throws InvalidConfigurationException
    {
        final var cluster = clusterRegistry.findCluster(
            vmSpec.poolLabel(), vmSpec.zone(), ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vmSpec.poolLabel() + " and zone " + vmSpec.zone());
        }
        return new Workload(
            "init-request-tunnel",
            "networld/grpcurl:latest",
            Collections.emptyMap(),
            List.of(
                "grpcurl",
                "--plaintext",
                "-d",
                String.format(
                    "{\"remote_v6_address\": \"%s\", \"worker_pod_v4_address\": \"%s\", \"k8s_v4_pod_cidr\": \"%s\", }",
                    remoteV6,
                    "$LZY_VM_IP_ADDRESS",
                    clusterRegistry.getClusterPodsCidr(cluster.clusterId())
                ),
                "178.154.200.115:8444",
                "ai.lzy.v1.tunnel.LzyTunnelAgent/CreateTunnel"
            ),
            Collections.emptyMap(),
            Collections.emptyList(),
            true
        );
    }
}
