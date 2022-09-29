package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.alloc.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.dao.VmDao;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.volume.DiskVolumeDescription;
import ai.lzy.allocator.volume.HostPathVolumeDescription;
import ai.lzy.allocator.volume.KuberVolumeManager;
import ai.lzy.allocator.volume.VolumeClaim;
import ai.lzy.model.db.TransactionHandle;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.defaultRetryPolicy;
import static ai.lzy.model.db.DbHelper.withRetries;

@Singleton
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
public class KuberVmAllocator implements VmAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberVmAllocator.class);
    private static final String NAMESPACE = "default";

    private static final String NAMESPACE_KEY = "namespace";
    private static final String POD_NAME_KEY = "pod-name";
    private static final String CLUSTER_ID_KEY = "cluster-id";
    public static final String VM_POD_NAME_PREFIX = "lzy-vm-";

    private final VmDao dao;
    private final ClusterRegistry poolRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;

    @Inject
    public KuberVmAllocator(VmDao dao, ClusterRegistry poolRegistry, KuberClientFactory factory, ServiceConfig config) {
        this.dao = dao;
        this.poolRegistry = poolRegistry;
        this.factory = factory;
        this.config = config;
    }

    @Override
    public void allocate(Vm.Spec vmSpec) throws InvalidConfigurationException {
        final var cluster = poolRegistry.findCluster(
            vmSpec.poolLabel(), vmSpec.zone(), ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + vmSpec.poolLabel() + " and zone " + vmSpec.zone());
        }

        try (final var client = factory.build(cluster)) {
            var podSpecBuilder = new PodSpecBuilder(
                vmSpec, client, config, PodSpecBuilder.VM_POD_TEMPLATE_PATH, VM_POD_NAME_PREFIX
            );
            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> dao.saveAllocatorMeta(
                    vmSpec.vmId(),
                    Map.of(
                        NAMESPACE_KEY, NAMESPACE,
                        POD_NAME_KEY, podSpecBuilder.getPodName(),
                        CLUSTER_ID_KEY, cluster.clusterId()),
                    null),
                RuntimeException::new);

            final List<DiskVolumeDescription> diskVolumeDescriptions = vmSpec.volumeRequests().stream()
                .filter(volumeRequest -> volumeRequest.volumeDescription() instanceof DiskVolumeDescription)
                .map(volumeRequest -> (DiskVolumeDescription) volumeRequest.volumeDescription())
                .toList();
            final List<VolumeClaim> volumeClaims = KuberVolumeManager.allocateVolumes(client, diskVolumeDescriptions);
            dao.setVolumeClaims(vmSpec.vmId(), volumeClaims, null);

            final Pod vmPodSpec = podSpecBuilder
                .withWorkloads(vmSpec.workloads())
                .withVolumes(volumeClaims)
                .withHostVolumes(vmSpec.volumeRequests().stream()
                    .filter(v -> v.volumeDescription() instanceof HostPathVolumeDescription)
                    .map(v -> (HostPathVolumeDescription) v.volumeDescription())
                    .toList())
                .build();

            vmPodSpec.getSpec()
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
            boolean needTunnel = true;
            if (needTunnel) {
                vmPodSpec.getSpec()
                    .getAffinity()
                    .getPodAffinity()
                    .getRequiredDuringSchedulingIgnoredDuringExecution()
                    .add(
                        new PodAffinityTermBuilder()
                            .withLabelSelector(
                                new LabelSelectorBuilder()
                                    .withMatchExpressions(
                                        new LabelSelectorRequirementBuilder()
                                            .withKey("lzy.ai/app")
                                            .withOperator("In")
                                            .withValues("tunnel")
                                            .build()
                                    ).build()
                            ).build()
                    );
            }
            LOG.debug("Creating pod with podspec: {}", vmPodSpec);

            final Pod pod;
            try {
                pod = client.pods()
                    .inNamespace(NAMESPACE)
                    .resource(vmPodSpec)
                    .create();
            } catch (Exception e) {
                LOG.error("Failed to allocate vm pod: {}", e.getMessage(), e);
                deallocate(vmSpec.vmId());
                //TODO (tomato): add retries here if the error is caused due to temporal problems with kuber
                throw new RuntimeException("Failed to allocate vm pod: " + e.getMessage(), e);
            }
            LOG.debug("Created vm pod in Kuber: {}", pod);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Database error: " + e.getMessage(), e);
        }
    }

    @Nullable
    private Pod getPod(String namespace, String name, KubernetesClient client) {
        final var podsList = client.pods()
            .inNamespace(namespace)
            .list(new ListOptionsBuilder()
                .withLabelSelector(KuberLabels.LZY_POD_NAME_LABEL + "=" + name)
                .build()
            ).getItems();
        if (podsList.size() < 1) {
            return null;
        }
        final var podSpec = podsList.get(0);
        if (podSpec.getMetadata() != null
            && podSpec.getMetadata().getName() != null
            && podSpec.getMetadata().getName().equals(name))
        {
            return podSpec;
        }
        return null;
    }

    @Override
    public void deallocate(String vmId) {
        var meta = withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> dao.getAllocatorMeta(vmId, null),
            ex -> new RuntimeException("Database error: " + ex.getMessage(), ex));

        if (meta == null) {
            throw new RuntimeException("Cannot get allocator metadata for vmId " + vmId);
        }

        final var clusterId = meta.get(CLUSTER_ID_KEY);
        final var credentials = poolRegistry.getCluster(clusterId);
        final var ns = meta.get(NAMESPACE_KEY);
        final var podName = meta.get(POD_NAME_KEY);

        try (final var client = factory.build(credentials)) {
            final var pod = getPod(ns, podName, client);
            if (pod != null) {
                client.pods()
                    .inNamespace(ns)
                    .resource(pod)
                    .delete();
            } else {
                LOG.warn("Pod with name {} not found", podName);
            }

            withRetries(
                defaultRetryPolicy(),
                LOG,
                () -> KuberVolumeManager.freeVolumes(client, dao.getVolumeClaims(vmId, null)),
                ex -> new RuntimeException("Database error: " + ex.getMessage(), ex));
        }
    }

    @Override
    public List<VmEndpoint> getVmEndpoints(String vmId, @Nullable TransactionHandle transaction) {
        final List<VmEndpoint> hosts = new ArrayList<>();

        final var meta = withRetries(
            defaultRetryPolicy(),
            LOG,
            () -> dao.getAllocatorMeta(vmId, transaction),
            ex -> new RuntimeException("Database error: " + ex.getMessage(), ex));

        if (meta == null) {
            throw new RuntimeException("Cannot get allocator metadata for vmId " + vmId);
        }

        final var clusterId = meta.get(CLUSTER_ID_KEY);
        final var credentials = poolRegistry.getCluster(clusterId);
        final var ns = meta.get(NAMESPACE_KEY);
        final var podName = meta.get(POD_NAME_KEY);

        try (final var client = factory.build(credentials)) {
            final var pod = getPod(ns, podName, client);
            if (pod != null) {
                final var nodeName = pod.getSpec().getNodeName();
                final var node = client.nodes()
                        .withName(nodeName)
                        .get();

                for (final var address: node.getStatus().getAddresses()) {
                    final var type = switch (address.getType().toLowerCase()) {
                        case "hostname" -> VmEndpointType.HOST_NAME;
                        case "internalip" -> VmEndpointType.INTERNAL_IP;
                        case "externalip" -> VmEndpointType.EXTERNAL_IP;
                        default -> throw new RuntimeException("Undefined type of node address: " + address.getType());
                    };
                    hosts.add(new VmEndpoint(type, address.getAddress()));
                }

            } else {
                throw new RuntimeException("Cannot get pod with name " + podName + " to get addresses");
            }
        }
        return hosts;
    }
}
