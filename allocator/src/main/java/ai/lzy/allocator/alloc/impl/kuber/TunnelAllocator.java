package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.Pod;
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
    public static final String TUNNEL_POD_APP_LABEL_VALUE = "tunnel";

    private final ClusterRegistry clusterRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;

    public TunnelAllocator(ClusterRegistry clusterRegistry, KuberClientFactory factory, ServiceConfig config) {
        this.clusterRegistry = clusterRegistry;
        this.factory = factory;
        this.config = config;
    }

    /**
     * Creates tunnel k8s pod with host network, which must be on the same node with corresponding vm k8s pod.
     * The tunnel pod must contain {@link ai.lzy.tunnel.TunnelAgentMain}.
     *
     * @param vmSpec - Spec of the VM to create tunnel from.
     * @throws InvalidConfigurationException - if allocator cannot find suit cluster for the vm spec.
     */
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
            ).withPodAntiAffinity(
                KuberLabels.LZY_POD_SESSION_ID_LABEL, "NotIn", vmSpec.sessionId()
            ).withPodAntiAffinity(
                KuberLabels.LZY_APP_LABEL, "In", vmSpec.sessionId(), TUNNEL_POD_APP_LABEL_VALUE
            ).build();

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

    /**
     * Constructs the {@link Workload} with the init container, which will request tunnel creation to the tunnel pod,
     * who must be created by the {@link TunnelAllocator#allocateTunnel(Vm.Spec)} method.
     *
     * @param remoteV6  - v6 address of the another end of the tunnel.
     * @param poolLabel - lzy vm pool label for the pod.
     * @param zone      - zone label for the pod.
     * @return {@link Workload} with the init container.
     * @throws InvalidConfigurationException if allocator cannot find suit cluster for the vm spec.
     */
    public Workload createRequestTunnelInitContainer(String remoteV6, String poolLabel, String zone)
        throws InvalidConfigurationException {
        final var cluster = clusterRegistry.findCluster(
            poolLabel, zone, ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + poolLabel + " and zone " + zone);
        }
        return new Workload(
            "init-request-tunnel",
            config.getTunnelRequestContainerImage(),
            Collections.emptyMap(),
            List.of(
                "./grpcurl",
                "--plaintext",
                "-d",
                String.format(
                    "{\"remote_v6_address\": \"%s\", \"worker_pod_v4_address\": \"%s\", \"k8s_v4_pod_cidr\": \"%s\"}",
                    remoteV6,
                    String.format("$(%s)", AllocatorAgent.VM_IP_ADDRESS),
                    clusterRegistry.getClusterPodsCidr(cluster.clusterId())
                ),
                String.format("$(%s):1234", AllocatorAgent.VM_NODE_IP_ADDRESS),
                "ai.lzy.v1.tunnel.LzyTunnelAgent/CreateTunnel"
            ),
            Collections.emptyMap(),
            Collections.emptyList(),
            true
        );
    }
}
