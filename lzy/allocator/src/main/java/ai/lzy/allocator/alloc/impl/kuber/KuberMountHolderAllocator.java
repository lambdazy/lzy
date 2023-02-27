package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.allocator.alloc.VmAllocator;
import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.vmpool.VmPoolRegistry;
import io.fabric8.kubernetes.api.model.Pod;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ai.lzy.allocator.alloc.impl.kuber.PodSpecBuilder.MOUNT_HOLDER_POD_TEMPLATE_PATH;
import static ai.lzy.allocator.alloc.impl.kuber.PodSpecBuilder.TUNNEL_POD_TEMPLATE_PATH;

@Singleton
@Requires(property = "allocator.kuber-mount-holder-allocator.enabled", value = "true")
public class KuberMountHolderAllocator implements MountHolderAllocator {
    private static final Logger LOG = LogManager.getLogger(KuberMountHolderAllocator.class);

    public static final String NAMESPACE_VALUE = "default";
    public static final String MOUNT_HOLDER_POD_NAME_PREFIX = "lzy-mount-holder-";

    private final ClusterRegistry clusterRegistry;
    private final VmPoolRegistry poolRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;

    public KuberMountHolderAllocator(ClusterRegistry clusterRegistry, VmPoolRegistry poolRegistry,
                                     KuberClientFactory factory, ServiceConfig config) {
        this.clusterRegistry = clusterRegistry;
        this.poolRegistry = poolRegistry;
        this.factory = factory;
        this.config = config;
    }

    @Override
    public String allocateMountHolder(VolumeClaim volumeClaim, Vm.Spec mountToVm) throws InvalidConfigurationException {
        final var cluster = clusterRegistry.findCluster(
            mountToVm.poolLabel(), mountToVm.zone(), ClusterRegistry.ClusterType.User);
        final var pool = poolRegistry.findPool(mountToVm.poolLabel());
        if (cluster == null || pool == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + mountToVm.poolLabel() + " and zone " + mountToVm.zone());
        }

        String diskId = volumeClaim.volume().diskId();
        String vmId = mountToVm.vmId();
        String podName = MOUNT_HOLDER_POD_NAME_PREFIX + diskId;

        try (final var client = factory.build(cluster)) {
            var mountHolderPodBuilder = new PodSpecBuilder(podName, MOUNT_HOLDER_POD_TEMPLATE_PATH, client, config);
            var mountHolderWorkload = createWorkload(diskId, mountToVm.poolLabel(), mountToVm.zone());
            var hostVolume = new HostPathVolumeDescription("host-path-volume-" + UUID.randomUUID(), "base-volume",
                "/mnt", HostPathVolumeDescription.HostPathType.DIRECTORY);

            var podSpec = mountHolderPodBuilder
                .withWorkloads(List.of(mountHolderWorkload), false)
                .withVolumes(List.of(volumeClaim))
                .withHostVolumes(List.of(hostVolume))
                .withPodAffinity(KuberLabels.LZY_VM_ID_LABEL, "In", vmId)
                .build();

            final Pod pod;
            try {
                pod = client.pods().inNamespace(NAMESPACE_VALUE).resource(podSpec).create();
            } catch (Exception e) {
                if (KuberUtils.isResourceAlreadyExist(e)) {
                    LOG.warn("Mount holder allocation request for disk {} to vm {} already exist", diskId, vmId);
                }

                LOG.error("Failed to allocate pod {}: {}", podName, e.getMessage(), e);

                throw new RuntimeException();
                //return VmAllocator.Result.FAILED.withReason(
                //    "Failed to allocate vm (vmId: %s) pod: %s".formatted(vmSpec.vmId(), e.getMessage()));
            }
            LOG.debug("Created mount holder pod in Kuber: {}", pod);

            return podName;
        }
    }

    @Override
    public void deallocateMountHolder(String podName) {

    }

    public Workload createWorkload(String diskId, String poolLabel, String zone)
        throws InvalidConfigurationException
    {
        final var cluster = clusterRegistry.findCluster(poolLabel, zone, ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new InvalidConfigurationException(
                "Cannot find pool for label " + poolLabel + " and zone " + zone);
        }

        final var clusterPodsCidr = clusterRegistry.getClusterPodsCidr(cluster.clusterId());
        final boolean readOnlyMount = false;

        final List<VolumeMount> mounts = new ArrayList<>();
        mounts.add(new VolumeMount("base-volume", "/mnt",
            readOnlyMount, VolumeMount.MountPropagation.BIDIRECTIONAL));
        mounts.add(new VolumeMount("disk-" + diskId, "/mnt/volume-" + diskId,
            readOnlyMount, VolumeMount.MountPropagation.BIDIRECTIONAL));

        return new Workload(
            "mount-holder",
            config.getMountHolderImage(),
            Map.of(),
            List.of("sh", "-c", "tail -f /dev/null"),
            Map.of(),
            mounts
        );
    }

}
