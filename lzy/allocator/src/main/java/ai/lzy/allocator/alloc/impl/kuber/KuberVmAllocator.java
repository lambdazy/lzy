package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.dao.VmDao;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.HostPathVolumeDescription;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.model.debug.InjectedFailures;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.volume.KuberVolumeManager;
import ai.lzy.longrunning.dao.OperationCompletedException;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.TransactionHandle;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Named;

import static ai.lzy.allocator.alloc.impl.kuber.PodSpecBuilder.VM_POD_TEMPLATE_PATH;
import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;
import static java.util.Objects.requireNonNull;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class KuberVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberVmAllocator.class);

    public static final String NAMESPACE_KEY = "namespace";
    public static final String NAMESPACE_VALUE = "default";
    public static final String POD_NAME_KEY = "pod-name";
    public static final String NODE_NAME_KEY = "node-name";
    public static final String NODE_INSTANCE_ID_KEY = "node-instance-id";
    public static final String CLUSTER_ID_KEY = "cluster-id";
    public static final String VM_POD_NAME_PREFIX = "lzy-vm-";
    public static final String VM_POD_APP_LABEL_VALUE = "vm";

    private final VmDao vmDao;
    private final OperationDao operationsDao;
    private final ClusterRegistry poolRegistry;
    private final KuberClientFactory factory;
    private final NodeRemover nodeRemover;
    private final ServiceConfig config;

    @Inject
    public KuberVmAllocator(VmDao vmDao, @Named("AllocatorOperationDao") OperationDao operationsDao,
                            ClusterRegistry poolRegistry, KuberClientFactory factory, NodeRemover nodeRemover,
                            ServiceConfig config)
    {
        this.vmDao = vmDao;
        this.operationsDao = operationsDao;
        this.poolRegistry = poolRegistry;
        this.factory = factory;
        this.nodeRemover = nodeRemover;
        this.config = config;
    }

    @Override
    public boolean allocate(Vm vm) throws InvalidConfigurationException {
        var cluster = poolRegistry.findCluster(vm.poolLabel(), vm.zone(), vm.spec().clusterType());

        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vm.poolLabel() + " and zone " + vm.zone());
        }

        final var vmSpec = vm.spec();
        var allocState = vm.allocateState();

        try (final var client = factory.build(cluster)) {
            var podSpecBuilder = new PodSpecBuilder(vmSpec, client, config, VM_POD_TEMPLATE_PATH, VM_POD_NAME_PREFIX);

            final String podName = podSpecBuilder.getPodName();

            if (allocState.allocatorMeta() == null) {
                final var allocatorMeta = Map.of(
                    NAMESPACE_KEY, NAMESPACE_VALUE,
                    POD_NAME_KEY, podName,
                    CLUSTER_ID_KEY, cluster.clusterId());

                try {
                    withRetries(LOG, () -> vmDao.setAllocatorMeta(vmSpec.vmId(), allocatorMeta, null));
                    allocState = allocState.withAllocatorMeta(allocatorMeta);
                } catch (Exception ex) {
                    LOG.error("Cannot save allocator meta for operation {} (VM {}): {}",
                        vm.allocOpId(), vm.vmId(), ex.getMessage());
                    return false; // retry later
                }

                try {
                    withRetries(LOG, () -> operationsDao.update(vm.allocOpId(), null));
                } catch (OperationCompletedException e) {
                    LOG.error("Cannot update operation {} (VM {}): already completed", vm.allocOpId(), vm.vmId());
                    return true;
                } catch (Exception ex) {
                    LOG.error("Cannot update operation {} (VM {}): {}", vm.allocOpId(), vm.vmId(), ex.getMessage());
                    return false;
                }
            } else {
                LOG.info("Found exising allocator meta {} for VM {}", allocState.allocatorMeta(), vmSpec.vmId());
                // TODO: validate existing meta
            }

            InjectedFailures.failAllocateVm6();

            if (allocState.volumeClaims() == null) {
                final var resourceVolumes = vmSpec.volumeRequests().stream()
                    .filter(request -> request.volumeDescription() instanceof VolumeRequest.ResourceVolumeDescription)
                    .map(request -> (VolumeRequest.ResourceVolumeDescription) request.volumeDescription())
                    .toList();

                final var volumeClaims = KuberVolumeManager.allocateVolumes(client, resourceVolumes);

                InjectedFailures.failAllocateVm7();

                try {
                    withRetries(LOG, () -> vmDao.setVolumeClaims(vmSpec.vmId(), volumeClaims, null));
                    allocState = allocState.withVolumeClaims(volumeClaims);
                } catch (Exception ex) {
                    LOG.error("Cannot save volume claims for operation {} (VM {}): {}",
                        vm.allocOpId(), vm.vmId(), ex.getMessage());
                    return false; // retry later
                }

                try {
                    withRetries(LOG, () -> operationsDao.update(vm.allocOpId(), null));
                } catch (OperationCompletedException e) {
                    LOG.error("Cannot update operation {} (VM {}): already completed", vm.allocOpId(), vm.vmId());
                    return true;
                } catch (Exception ex) {
                    LOG.error("Cannot update operation {} (VM {}): {}", vm.allocOpId(), vm.vmId(), ex.getMessage());
                    return false;
                }
            } else {
                LOG.info("Found existing volumes claims for VM {}: {}",
                    vmSpec.vmId(),
                    allocState.volumeClaims().stream().map(VolumeClaim::name).collect(Collectors.joining(", ")));
            }

            // add k8s pod affinity to allocate vm pod on the node with the tunnel pod,
            // which must be allocated by TunnelAllocator#allocateTunnel method
            if (vmSpec.proxyV6Address() != null) {
                podSpecBuilder = podSpecBuilder.withPodAffinity(
                    KuberLabels.LZY_APP_LABEL, "In", KuberTunnelAllocator.TUNNEL_POD_APP_LABEL_VALUE);

                InjectedFailures.failAllocateVm8();
            }

            final String vmOtt = allocState.vmOtt();

            final Pod vmPodSpec = podSpecBuilder
                .withWorkloads(vmSpec.initWorkloads(), true)
                .withWorkloads(
                    vmSpec.workloads().stream()
                        // we pass vmOtt to _all_ workloads, but only _one_ of them will use it
                        .map(wl -> wl.withEnv(AllocatorAgent.VM_ALLOCATOR_OTT, vmOtt))
                        .toList(),
                    false)
                .withVolumes(requireNonNull(allocState.volumeClaims()))
                .withHostVolumes(vmSpec.volumeRequests().stream()
                    .filter(v -> v.volumeDescription() instanceof HostPathVolumeDescription)
                    .map(v -> (HostPathVolumeDescription) v.volumeDescription())
                    .toList())
                // not to be allocated with another vm
                .withPodAntiAffinity(KuberLabels.LZY_APP_LABEL, "In", VM_POD_APP_LABEL_VALUE)
                // not to be allocated with pods from other session
                .withPodAntiAffinity(KuberLabels.LZY_POD_SESSION_ID_LABEL, "NotIn", vmSpec.sessionId())
                .build();

            LOG.debug("Creating pod with podspec: {}", vmPodSpec);

            final Pod pod;
            try {
                pod = client.pods().inNamespace(NAMESPACE_VALUE).resource(vmPodSpec).create();
            } catch (Exception e) {
                if (KuberUtils.isResourceAlreadyExist(e)) {
                    LOG.warn("Allocation request for VM {} already exist", vmSpec.vmId());
                    return true;
                }

                LOG.error("Failed to allocate vm pod: {}", e.getMessage(), e);

                throw new RuntimeException(
                    "Failed to allocate vm (vmId: %s) pod: %s".formatted(vmSpec.vmId(), e.getMessage()), e);
            }
            LOG.debug("Created vm pod in Kuber: {}", pod);

            InjectedFailures.failAllocateVm9();

            return true;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Allocation error: " + e.getMessage(), e);
        }
    }

    @Nullable
    private Pod getVmPod(String namespace, String name, KubernetesClient client) {
        final var podsList = client.pods()
            .inNamespace(namespace)
            .list(new ListOptionsBuilder()
                .withLabelSelector(KuberLabels.LZY_POD_NAME_LABEL + "=" + name)
                .build())
            .getItems();
        if (podsList.size() < 1) {
            return null;
        }
        final var podSpec = podsList.get(0);
        if (podSpec.getMetadata() != null &&
            podSpec.getMetadata().getName() != null &&
            podSpec.getMetadata().getName().equals(name))
        {
            return podSpec;
        }
        return null;
    }

    /**
     * Find all pods with label "lzy.ai/vm-id"=<code>vmId</code>. It is expected to be the corresponding <code>vm</code>
     * pod and optionally the <code>tunnel</code> pod on the same k8s node, if it exists. In the future, we may add
     * other system pods necessary for this vm (for example, pod with mounted disc).
     *
     * @param namespace - k8s namespace with pods
     * @param vmId      - id of vm
     * @param client    - k8s client
     * @return k8s pods in <code>namespace</code> with label "lzy.ai/vm-id"=<code>vmId</code> got by <code>client</code>
     */
    @Nullable
    private List<Pod> getAllPodsWithVmId(String namespace, String vmId, KubernetesClient client) {
        return client.pods()
                .inNamespace(namespace)
                .list(new ListOptionsBuilder()
                        .withLabelSelector(KuberLabels.LZY_VM_ID_LABEL + "=" + vmId)
                        .build()
                ).getItems();
    }

    /**
     * Deallocates all pods with label "lzy.ai/vm-id"=<code>vmId</code>. It is expected to be the corresponding
     * <code>vm</code> pod and optionally the <code>tunnel</code> pod on the same k8s node, if it exists. In the future,
     * we may add other system pods necessary for this vm (for example, pod with mounted disc).
     *
     * @param vmId - id of vm to deallocate
     */
    @Override
    public void deallocate(String vmId) {
        var meta = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> vmDao.getAllocatorMeta(vmId, null),
                ex -> new RuntimeException("Database error: " + ex.getMessage(), ex));

        if (meta == null) {
            LOG.warn("Cannot get allocator metadata for vmId {}", vmId);
            return;
        }

        final var clusterId = meta.get(CLUSTER_ID_KEY);
        final var credentials = poolRegistry.getCluster(clusterId);
        final var ns = meta.get(NAMESPACE_KEY);

        try (final var client = factory.build(credentials)) {
            List<StatusDetails> statusDetails = client.pods()
                    .inNamespace(ns)
                    .withLabelSelector(KuberLabels.LZY_VM_ID_LABEL + "=" + vmId)
                    .delete();
            if (statusDetails.isEmpty()) {
                LOG.warn("No delete status details were provided by k8s client after deleting pods with vm {}", vmId);
            }

            var claims = vmDao.getVolumeClaims(vmId, null);
            if (!claims.isEmpty()) {
                KuberVolumeManager.freeVolumes(client, claims);
            }
        } catch (SQLException e) {
            LOG.error("Cannot read volume claims for vm {}: {}", vmId, e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        var nodeName = meta.get(NODE_NAME_KEY);
        var nodeInstanceId = meta.get(NODE_INSTANCE_ID_KEY);

        if (nodeInstanceId == null) {
            LOG.error("Cannot delete node for VM {} (node {}): unknown", vmId, nodeName);
            return;
        }

        // TODO(artolord) make optional deletion of system nodes
        if (credentials.type().equals(ClusterRegistry.ClusterType.User)) {
            nodeRemover.removeNode(vmId, nodeName, nodeInstanceId);
        }
    }

    @Override
    public List<VmEndpoint> getVmEndpoints(String vmId, @Nullable TransactionHandle transaction) {
        final List<VmEndpoint> hosts = new ArrayList<>();

        final var meta = withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> vmDao.getAllocatorMeta(vmId, transaction),
                ex -> new RuntimeException("Database error: " + ex.getMessage(), ex));

        if (meta == null) {
            throw new RuntimeException("Cannot get allocator metadata for vmId " + vmId);
        }

        final var clusterId = meta.get(CLUSTER_ID_KEY);
        final var credentials = poolRegistry.getCluster(clusterId);
        final var ns = meta.get(NAMESPACE_KEY);
        final var podName = meta.get(POD_NAME_KEY);

        try (final var client = factory.build(credentials)) {
            final var pod = getVmPod(ns, podName, client);
            if (pod != null) {
                final var nodeName = pod.getSpec().getNodeName();
                final var node = client.nodes().withName(nodeName).get();

                final var providerId = node.getSpec() != null ? node.getSpec().getProviderID() : null;
                final var instanceId = providerId != null && providerId.startsWith("yandex://")
                    ? providerId.substring("yandex://".length())
                    : null;

                LOG.info("VM {} is allocated at POD {} on node {} ({})", vmId, podName, nodeName, providerId);

                for (final var address : node.getStatus().getAddresses()) {
                    final var type = switch (address.getType().toLowerCase()) {
                        case "hostname" -> VmEndpointType.HOST_NAME;
                        case "internalip" -> VmEndpointType.INTERNAL_IP;
                        case "externalip" -> VmEndpointType.EXTERNAL_IP;
                        default -> throw new RuntimeException("Undefined type of node address: " + address.getType());
                    };
                    hosts.add(new VmEndpoint(type, address.getAddress()));
                }

                meta.put(NODE_NAME_KEY, nodeName);
                if (instanceId != null) {
                    meta.put(NODE_INSTANCE_ID_KEY, instanceId);
                }
            } else {
                throw new RuntimeException("Cannot get pod with name " + podName + " to get addresses");
            }
        }

        try {
            withRetries(LOG, () -> vmDao.setAllocatorMeta(vmId, meta, transaction));
        } catch (Exception e) {
            LOG.error("Cannot save updated allocator meta for vm {}: {}", vmId, e.getMessage());
        }

        return hosts;
    }
}
