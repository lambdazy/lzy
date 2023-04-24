package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.tunnel.LzyTunnelAgentGrpc;
import ai.lzy.v1.tunnel.TA;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static ai.lzy.allocator.alloc.impl.kuber.PodSpecBuilder.TUNNEL_POD_TEMPLATE_PATH;

@Singleton
@Requires(property = "allocator.kuber-tunnel-allocator.enabled", value = "true")
public class KuberTunnelAllocator implements TunnelAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberTunnelAllocator.class);
    private static final String NAMESPACE = "default";
    public static final String TUNNEL_POD_NAME_PREFIX = "lzy-tunnel-";
    public static final String TUNNEL_POD_APP_LABEL_VALUE = "tunnel";

    private final ClusterRegistry clusterRegistry;
    private final VmPoolRegistry poolRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;
    private final ServiceConfig.TunnelConfig tunnelConfig;

    public KuberTunnelAllocator(ClusterRegistry clusterRegistry, VmPoolRegistry poolRegistry,
                                KuberClientFactory factory, ServiceConfig config,
                                ServiceConfig.TunnelConfig tunnelConfig)
    {
        this.clusterRegistry = clusterRegistry;
        this.poolRegistry = poolRegistry;
        this.factory = factory;
        this.config = config;
        this.tunnelConfig = tunnelConfig;
    }

    /**
     * Creates tunnel k8s pod with host network, which must be on the same node with corresponding vm k8s pod.
     * The tunnel pod must contain {@link ai.lzy.tunnel.TunnelAgentMain}.
     *
     * @param vmSpec - Spec of the VM to create tunnel from.
     * @return allocated Pod name
     * @throws InvalidConfigurationException - if allocator cannot find suit cluster for the vm spec.
     */
    public String allocateTunnelAgent(Vm.Spec vmSpec) throws InvalidConfigurationException {
        final var cluster = clusterRegistry.findCluster(
            vmSpec.poolLabel(), vmSpec.zone(), ClusterRegistry.ClusterType.User);
        final var pool = poolRegistry.findPool(vmSpec.poolLabel());
        if (cluster == null || pool == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vmSpec.poolLabel() + " and zone " + vmSpec.zone());
        }

        try (final var client = factory.build(cluster)) {
            var tunnelPodName = TUNNEL_POD_NAME_PREFIX + vmSpec.vmId().toLowerCase(Locale.ROOT);
            var tunnelPodBuilder = new PodSpecBuilder(tunnelPodName, TUNNEL_POD_TEMPLATE_PATH, client, config);
            Pod tunnelPod = tunnelPodBuilder.withWorkloads(
                    List.of(new Workload("tunnel", tunnelConfig.getPodImage(), Map.of(), List.of(), Map.of(),
                        List.of())),
                    /* init */ false)
                // not to be allocated with another tunnel
                .withPodAntiAffinity(KuberLabels.LZY_APP_LABEL, "In", vmSpec.sessionId(), TUNNEL_POD_APP_LABEL_VALUE)
                // not to be allocated with pod form another session
                .withPodAntiAffinity(KuberLabels.LZY_POD_SESSION_ID_LABEL, "NotIn", vmSpec.sessionId())
                .withLabels(Map.of(
                    KuberLabels.LZY_POD_SESSION_ID_LABEL, vmSpec.sessionId()
                ))
                .withLoggingVolume()
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

    @Override
    public VmAllocator.Result deleteTunnel(String clusterId, String podName) {
        var cluster = clusterRegistry.getCluster(clusterId);
        try (var client = factory.build(cluster)) {
            var pod = client.pods()
                .inNamespace(NAMESPACE)
                .withName(podName)
                .get();
            var channel = GrpcUtils.newGrpcChannel(pod.getStatus().getPodIP(),
                tunnelConfig.getAgentPort(), LzyTunnelAgentGrpc.SERVICE_NAME);
            try {
                var tunnelAgent = LzyTunnelAgentGrpc.newBlockingStub(channel);
                var resp = tunnelAgent.deleteTunnel(TA.DeleteTunnelRequest.getDefaultInstance());
            } catch (StatusRuntimeException e) {
                LOG.warn("Couldn't delete tunnel on tunnel agent", e);
                return VmAllocator.Result.fromGrpcStatus(e.getStatus());
            } finally {
                channel.shutdown();
            }
            return VmAllocator.Result.SUCCESS;
        } catch (KubernetesClientException clientException) {
            if (clientException.getCode() == HttpStatus.NOT_FOUND.getCode()) {
                LOG.warn("Pod {} doesn't exist", podName);
                return VmAllocator.Result.SUCCESS;
            }

            LOG.error("Couldn't delete tunnel on pod {} from cluster {}: {}", podName, clusterId,
                clientException);
            return VmAllocator.Result.RETRY_LATER;
        }
    }

    @Override
    public VmAllocator.Result deallocateTunnelAgent(String clusterId, String podName) {
        var cluster = clusterRegistry.getCluster(clusterId);
        try (var client = factory.build(cluster)) {
            var details = client.pods()
                .inNamespace(NAMESPACE)
                .withName(podName)
                .delete();
            if (details.isEmpty()) {
                LOG.warn("No status details for deleting pod {} from cluster {}", podName, clusterId);
            }
            return VmAllocator.Result.SUCCESS;
        } catch (KubernetesClientException clientException) {
            if (clientException.getCode() == HttpStatus.NOT_FOUND.getCode()) {
                LOG.warn("Pod {} doesn't exist in cluster {}", podName, clusterId);
                return VmAllocator.Result.SUCCESS;
            }

            LOG.error("Couldn't deallocate tunnel agent on pod {} from cluster {}: {}", podName, clusterId,
                clientException);
            return VmAllocator.Result.RETRY_LATER.withReason(clientException.getMessage());
        }
    }

    /**
     * Constructs the {@link Workload}, which will request tunnel creation to the tunnel pod,
     * who must be created by the {@link KuberTunnelAllocator#allocateTunnelAgent(Vm.Spec)} method.
     *
     * @param tunnelSettings - settings for tunnel setup
     * @param poolLabel      - lzy vm pool label for the pod.
     * @param zone           - zone label for the pod.
     * @return {@link Workload} with the init container.
     * @throws InvalidConfigurationException if allocator cannot find suit cluster for the vm spec.
     */
    public Workload createRequestTunnelWorkload(Vm.TunnelSettings tunnelSettings, String poolLabel, String zone)
        throws InvalidConfigurationException
    {
        final var cluster = clusterRegistry.findCluster(poolLabel, zone, ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + poolLabel + " and zone " + zone);
        }

        final var clusterPodsCidr = clusterRegistry.getClusterPodsCidr(cluster.clusterId());

        String jsonRequest = prepareCreateTunnelJsonRequest(tunnelSettings, clusterPodsCidr);
        return new Workload(
            "request-tunnel",
            tunnelConfig.getRequestContainerImage(),
            Map.of(),
            List.of(
                tunnelConfig.getRequestContainerGrpCurlPath(),
                "--plaintext",
                "-d",
                jsonRequest,
                "$(%s):%d"
                    .formatted(AllocatorAgent.VM_NODE_IP_ADDRESS, tunnelConfig.getAgentPort()),
                "ai.lzy.v1.tunnel.LzyTunnelAgent/CreateTunnel"
            ),
            Map.of(),
            List.of()
        );
    }

    private static String prepareCreateTunnelJsonRequest(Vm.TunnelSettings tunnelSettings, String clusterPodsCidr) {
        try {
            var request = TA.CreateTunnelRequest.newBuilder()
                    .setRemoteV6Address(tunnelSettings.proxyV6Address().getHostAddress())
                    .setTunnelIndex(tunnelSettings.tunnelIndex())
                    .setWorkerPodV4Address("$(" + AllocatorAgent.VM_IP_ADDRESS + ")") //for bash resolving
                    .setK8SV4PodCidr(clusterPodsCidr)
                    .build();
            return JsonFormat.printer()
                    .preservingProtoFieldNames()
                    .print(request);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Couldn't parse createTunnel request from protobuf to json", e);
            throw new RuntimeException(e);
        }
    }
}
