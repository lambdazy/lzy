package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nonnull;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static ai.lzy.allocator.alloc.impl.kuber.PodSpecBuilder.MOUNT_HOLDER_POD_TEMPLATE_PATH;

@Singleton
@Requires(property = "allocator.mount.enabled", value = "true")
public class KuberMountHolderManager implements MountHolderManager {
    private static final Logger LOG = LogManager.getLogger(KuberMountHolderManager.class);

    public static final String NAMESPACE_VALUE = "default";
    public static final String MOUNT_HOLDER_POD_NAME_PREFIX = "lzy-mount-holder-";
    public static final String HOST_VOLUME_NAME = "base-volume";

    private final ClusterRegistry clusterRegistry;
    private final KuberClientFactory factory;
    private final ServiceConfig config;
    private final ServiceConfig.MountConfig mountConfig;
    private final IdGenerator idGenerator;

    public KuberMountHolderManager(ClusterRegistry clusterRegistry, KuberClientFactory factory, ServiceConfig config,
                                   ServiceConfig.MountConfig mountConfig,
                                   @Named("AllocatorIdGenerator") IdGenerator idGenerator)
    {
        this.clusterRegistry = clusterRegistry;
        this.factory = factory;
        this.config = config;
        this.mountConfig = mountConfig;
        this.idGenerator = idGenerator;
    }

    @Override
    public ClusterPod allocateMountHolder(Vm.Spec mountToVm, List<DynamicMount> mounts) {
        final var cluster = getCluster(mountToVm.poolLabel(), mountToVm.zone());

        String vmId = mountToVm.vmId();
        String podName = mountHolderName(vmId);

        try (final var client = factory.build(cluster)) {
            var mountHolderPodBuilder = new PodSpecBuilder(podName, MOUNT_HOLDER_POD_TEMPLATE_PATH, client, config);
            var mountHolderWorkload = createWorkload(mounts);
            var hostVolume = createHostPathVolume(mountConfig);

            var podSpec = mountHolderPodBuilder
                .withWorkloads(List.of(mountHolderWorkload), false)
                .withHostVolumes(List.of(hostVolume))
                .withPodAffinity(KuberLabels.LZY_VM_ID_LABEL, "In", vmId)
                .withLabels(Map.of(
                    KuberLabels.LZY_POD_SESSION_ID_LABEL, mountToVm.sessionId()
                ))
                .withDynamicVolumes(mounts)
                .build();

            final Pod pod;
            try {
                pod = client.pods().inNamespace(NAMESPACE_VALUE).resource(podSpec).create();
            } catch (KubernetesClientException e) {
                if (KuberUtils.isResourceAlreadyExist(e)) {
                    LOG.warn("Mount holder allocation request for vm {} already exist", vmId);
                    return new ClusterPod(cluster.clusterId(), podName);
                }

                LOG.error("Failed to allocate pod {}: {}", podName, e.getMessage(), e);
                throw e;
            }
            LOG.debug("Created mount holder pod in Kuber: {}", pod);

            return new ClusterPod(cluster.clusterId(), podName);
        }
    }

    @Override
    public ClusterPod recreateWith(Vm.Spec vm, ClusterPod currentPod, List<DynamicMount> mounts) {
        deallocateMountHolder(currentPod);
        return allocateMountHolder(vm, mounts);
    }

    @Override
    public PodPhase checkPodPhase(ClusterPod clusterPod) {
        var cluster = clusterRegistry.getCluster(clusterPod.clusterId());
        try (final var client = factory.build(cluster)) {
            var pod = client.pods().inNamespace(NAMESPACE_VALUE).withName(clusterPod.podName()).get();
            if (pod == null) {
                LOG.warn("Pod {} not found", clusterPod.podName());
                return PodPhase.UNKNOWN;
            }
            return PodPhase.fromString(pod.getStatus().getPhase());
        } catch (KubernetesClientException e) {
            LOG.error("Failed to check pod phase {}: {}", clusterPod.podName(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deallocateMountHolder(ClusterPod clusterPod) {
        var cluster = getCluster(clusterPod.clusterId());
        try (final var client = factory.build(cluster)) {
            client.pods().inNamespace(NAMESPACE_VALUE).withName(clusterPod.podName()).delete();
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.warn("Pod {} is not found", clusterPod.podName());
                return;
            }
            throw e;
        }
    }

    private Workload createWorkload(List<DynamicMount> dynamicMounts) {
        final List<VolumeMount> mounts = new ArrayList<>(dynamicMounts.size() + 1);
        mounts.add(prepareVolumeMount(mountConfig));
        for (var dynamicMount : dynamicMounts) {
            var volumeMount = new VolumeMount(dynamicMount.id(), dynamicMount.mountPath(), false,
                VolumeMount.MountPropagation.BIDIRECTIONAL);
            mounts.add(volumeMount);
        }

        return new Workload(
            "mount-holder",
            mountConfig.getPodImage(),
            Map.of(),
            List.of("sh", "-c", "tail -f /dev/null"),
            Map.of(),
            mounts
        );
    }

    @Nonnull
    public static VolumeMount prepareVolumeMount(ServiceConfig.MountConfig mountConfig) {
        return new VolumeMount(HOST_VOLUME_NAME, mountConfig.getWorkerMountPoint(), false,
            VolumeMount.MountPropagation.BIDIRECTIONAL);
    }

    @Nonnull
    public static VolumeRequest createHostPathVolume(ServiceConfig.MountConfig mountConfig) {
        return new VolumeRequest(new RandomIdGenerator().generate("host-path-volume-", 20),
            new HostPathVolumeDescription(HOST_VOLUME_NAME, mountConfig.getHostMountPoint(),
                HostPathVolumeDescription.HostPathType.DIRECTORY_OR_CREATE));
    }

    @Nonnull
    private ClusterRegistry.ClusterDescription getCluster(String clusterId) {
        var cluster = clusterRegistry.getCluster(clusterId);
        if (cluster == null) {
            throw new IllegalStateException("Cluster " + clusterId + " is not found");
        }
        return cluster;
    }

    @Nonnull
    private ClusterRegistry.ClusterDescription getCluster(String poolLabel, String zone) {
        final var cluster = clusterRegistry.findCluster(poolLabel, zone, ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new IllegalStateException("Cannot find pool for label " + poolLabel + " and zone " + zone);
        }
        return cluster;
    }

    @Nonnull
    private String mountHolderName(String vmId) {
        return idGenerator.generate(MOUNT_HOLDER_POD_NAME_PREFIX + vmId.toLowerCase(Locale.ROOT) + "-");
    }

}
