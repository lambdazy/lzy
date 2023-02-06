package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import io.fabric8.kubernetes.api.model.Pod;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

import static ai.lzy.allocator.alloc.impl.kuber.PodSpecBuilder.TUNNEL_POD_TEMPLATE_PATH;

@Singleton
@Requires(property = "allocator.kuber-tunnel-allocator.enabled", value = "true")
public class KuberTunnelAllocator implements TunnelAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberTunnelAllocator.class);
    private static final String NAMESPACE = "default";
    private static final int TUNNEL_AGENT_PORT = 1234;
    public static final String TUNNEL_POD_NAME_PREFIX = "lzy-tunnel-";
    public static final String TUNNEL_POD_APP_LABEL_VALUE = "tunnel";

    private final ClusterRegistry clusterRegistry;
    private final VmPoolRegistry poolRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;

    public KuberTunnelAllocator(ClusterRegistry clusterRegistry, VmPoolRegistry poolRegistry,
                                KuberClientFactory factory, ServiceConfig config) {
        this.clusterRegistry = clusterRegistry;
        this.poolRegistry = poolRegistry;
        this.factory = factory;
        this.config = config;
    }

    /**
     * Creates tunnel k8s pod with host network, which must be on the same node with corresponding vm k8s pod.
     * The tunnel pod must contain {@link ai.lzy.tunnel.TunnelAgentMain}.
     *
     * @param vmSpec - Spec of the VM to create tunnel from.
     * @return allocated Pod name
     * @throws InvalidConfigurationException - if allocator cannot find suit cluster for the vm spec.
     */
    public String allocateTunnel(Vm.Spec vmSpec) throws InvalidConfigurationException {
        final var cluster = clusterRegistry.findCluster(
            vmSpec.poolLabel(), vmSpec.zone(), ClusterRegistry.ClusterType.User);
        final var pool = poolRegistry.findPool(vmSpec.poolLabel());
        if (cluster == null || pool == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vmSpec.poolLabel() + " and zone " + vmSpec.zone());
        }

        try (final var client = factory.build(cluster)) {
            var tunnelPodBuilder = new PodSpecBuilder(vmSpec, pool, client, config,
                TUNNEL_POD_TEMPLATE_PATH, TUNNEL_POD_NAME_PREFIX);
            Pod tunnelPod = tunnelPodBuilder.withWorkloads(
                    List.of(
                        new Workload("tunnel", config.getTunnelPodImage(), Map.of(), List.of(), Map.of(), List.of())),
                    /* init */ false)
                // not to be allocated with another tunnel
                .withPodAntiAffinity(KuberLabels.LZY_APP_LABEL, "In", vmSpec.sessionId(), TUNNEL_POD_APP_LABEL_VALUE)
                // not to be allocated with pod form another session
                .withPodAntiAffinity(KuberLabels.LZY_POD_SESSION_ID_LABEL, "NotIn", vmSpec.sessionId())
                .build();

            final var podName = tunnelPod.getMetadata().getName();

            try {
                tunnelPod = client.pods()
                    .inNamespace(NAMESPACE)
                    .resource(tunnelPod)
                    .create();
            } catch (Exception e) {
                if (KuberUtils.isResourceAlreadyExist(e)) {
                    LOG.warn("Tunnel pod {} already exists.", podName);
                    return podName;
                }

                LOG.error("Failed to allocate tunnel pod {}: {}", podName, e.getMessage(), e);
                throw new RuntimeException("Failed to allocate tunnel pod: " + e.getMessage(), e);
            }

            LOG.info("Created tunnel pod {} in Kuber: {}", podName, tunnelPod);
            return podName;
        }
    }

    /**
     * Constructs the {@link Workload}, which will request tunnel creation to the tunnel pod,
     * who must be created by the {@link KuberTunnelAllocator#allocateTunnel(Vm.Spec)} method.
     *
     * @param remoteV6  - v6 address of the another end of the tunnel.
     * @param poolLabel - lzy vm pool label for the pod.
     * @param zone      - zone label for the pod.
     * @return {@link Workload} with the init container.
     * @throws InvalidConfigurationException if allocator cannot find suit cluster for the vm spec.
     */
    public Workload createRequestTunnelWorkload(String remoteV6, String poolLabel, String zone)
        throws InvalidConfigurationException
    {
        final var cluster = clusterRegistry.findCluster(poolLabel, zone, ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + poolLabel + " and zone " + zone);
        }

        final var clusterPodsCidr = clusterRegistry.getClusterPodsCidr(cluster.clusterId());

        return new Workload(
            "request-tunnel",
            config.getTunnelRequestContainerImage(),
            Map.of(),
            List.of(
                config.getTunnelRequestContainerGrpCurlPath(),
                "--plaintext",
                "-d",
                "{\"remote_v6_address\": \"%s\", \"worker_pod_v4_address\": \"$(%s)\", \"k8s_v4_pod_cidr\": \"%s\"}"
                    .formatted(remoteV6, AllocatorAgent.VM_IP_ADDRESS, clusterPodsCidr),
                "$(%s):%d"
                    .formatted(AllocatorAgent.VM_NODE_IP_ADDRESS, TUNNEL_AGENT_PORT),
                "ai.lzy.v1.tunnel.LzyTunnelAgent/CreateTunnel"
            ),
            Map.of(),
            List.of()
        );
    }

    public void deallocateTunnel() {
        // TODO:
    }
}
