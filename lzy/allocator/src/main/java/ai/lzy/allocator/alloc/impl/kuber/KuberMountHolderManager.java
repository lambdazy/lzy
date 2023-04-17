package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.model.ClusterPod;
import ai.lzy.allocator.model.DynamicMount;
import ai.lzy.allocator.model.HostPathVolumeDescription;
import ai.lzy.allocator.model.PodPhase;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeMount;
import ai.lzy.allocator.model.Workload;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nonnull;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    public KuberMountHolderManager(ClusterRegistry clusterRegistry, KuberClientFactory factory, ServiceConfig config,
                                   ServiceConfig.MountConfig mountConfig)
    {
        this.clusterRegistry = clusterRegistry;
        this.factory = factory;
        this.config = config;
        this.mountConfig = mountConfig;
    }

    @Override
    public ClusterPod allocateMountHolder(Vm.Spec mountToVm) {
        final var cluster = getCluster(mountToVm.poolLabel(), mountToVm.zone());

        String vmId = mountToVm.vmId();
        String podName = mountHolderName(vmId);

        try (final var client = factory.build(cluster)) {
            var mountHolderPodBuilder = new PodSpecBuilder(podName, MOUNT_HOLDER_POD_TEMPLATE_PATH, client, config);
            var mountHolderWorkload = createWorkload();
            var hostVolume = createHostPathVolume(mountConfig);

            var podSpec = mountHolderPodBuilder
                .withWorkloads(List.of(mountHolderWorkload), false)
                .withHostVolumes(List.of(hostVolume))
                .withPodAffinity(KuberLabels.LZY_VM_ID_LABEL, "In", vmId)
                .withLabels(Map.of(
                    KuberLabels.LZY_POD_SESSION_ID_LABEL, mountToVm.sessionId()
                ))
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
    public void attachVolume(ClusterPod clusterPod, DynamicMount mount, VolumeClaim claim) {
        var cluster = getCluster(clusterPod.clusterId());
        try (final var client = factory.build(cluster)) {
            client.pods().inNamespace(NAMESPACE_VALUE).withName(clusterPod.podName())
                .edit(pod -> attachDiskToPodInPlace(pod, mount, claim));
        } catch (KubernetesClientException e) {
            LOG.error("Failed to attach volume to pod {}: {}", clusterPod.podName(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void detachVolume(ClusterPod clusterPod, String mountName) {
        var cluster = getCluster(clusterPod.clusterId());
        try (final var client = factory.build(cluster)) {
            client.pods().inNamespace(NAMESPACE_VALUE).withName(clusterPod.podName())
                .edit(pod -> detachDiskFromPodInPlace(pod, mountName));
        } catch (KubernetesClientException e) {
            LOG.error("Failed to detach volume from pod {}: {}", clusterPod.podName(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public PodPhase checkPodPhase(ClusterPod clusterPod) {
        var cluster = clusterRegistry.getCluster(clusterPod.clusterId());
        try (final var client = factory.build(cluster)) {
            var pod = client.pods().inNamespace(NAMESPACE_VALUE).withName(clusterPod.podName()).get();
            return PodPhase.fromString(pod.getStatus().getPhase());
        } catch (KubernetesClientException e) {
            LOG.error("Failed to check pod phase {}: {}", clusterPod.podName(), e.getMessage(), e);
            throw e;
        }
    }

    private Pod detachDiskFromPodInPlace(Pod pod, String mountName) {
        PodSpec spec = pod.getSpec();
        spec.getContainers()
            .forEach(container -> {
                var mounts = new ArrayList<>(container.getVolumeMounts());
                mounts.removeIf(mount -> mount.getName().equals(mountName));
                container.setVolumeMounts(mounts);
            });
        var volumes = new ArrayList<>(spec.getVolumes());
        volumes.removeIf(volume -> volume.getName().equals(mountName));
        spec.setVolumes(volumes);
        return pod;
    }

    @NotNull
    private Pod attachDiskToPodInPlace(Pod pod, DynamicMount mount, VolumeClaim claim) {
        PodSpec spec = pod.getSpec();
        spec.getContainers()
            .forEach(container -> {
                var mounts = new ArrayList<io.fabric8.kubernetes.api.model.VolumeMount>(
                    container.getVolumeMounts().size() + 1);
                mounts.addAll(container.getVolumeMounts());
                mounts.add(new VolumeMountBuilder()
                    .withName(mount.mountName())
                    .withMountPath(mountConfig.getWorkerMountPoint() + "/" + mount.mountPath())
                    .withReadOnly(false)
                    .withMountPropagation(VolumeMount.MountPropagation.BIDIRECTIONAL.asString())
                    .build());
                container.setVolumeMounts(mounts);
            });
        var volumes = ImmutableList.<Volume>builder()
            .addAll(spec.getVolumes())
            .add(new VolumeBuilder()
                .withName(mount.mountName())
                .withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource(claim.name(), false))
                .build()
            )
            .build();
        pod.getSpec().setVolumes(volumes);
        return pod;
    }

    @Override
    public void deallocateMountHolder(ClusterPod clusterPod) {
        // deallocate pod
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

    private Workload createWorkload() {
        final List<VolumeMount> mounts = new ArrayList<>(2);
        mounts.add(prepareVolumeMount(mountConfig));

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

    @NotNull
    public static HostPathVolumeDescription createHostPathVolume(ServiceConfig.MountConfig mountConfig) {
        return new HostPathVolumeDescription("host-path-volume-" + UUID.randomUUID(), HOST_VOLUME_NAME,
            mountConfig.getHostMountPoint(), HostPathVolumeDescription.HostPathType.DIRECTORY_OR_CREATE);
    }

    @NotNull
    private ClusterRegistry.ClusterDescription getCluster(String clusterId) {
        var cluster = clusterRegistry.getCluster(clusterId);
        if (cluster == null) {
            throw new IllegalStateException("Cluster " + clusterId + " is not found");
        }
        return cluster;
    }

    @NotNull
    private ClusterRegistry.ClusterDescription getCluster(String poolLabel, String zone) {
        final var cluster = clusterRegistry.findCluster(poolLabel, zone, ClusterRegistry.ClusterType.User);
        if (cluster == null) {
            throw new IllegalStateException("Cannot find pool for label " + poolLabel + " and zone " + zone);
        }
        return cluster;
    }

    @NotNull
    private static String mountHolderName(String vmId) {
        return MOUNT_HOLDER_POD_NAME_PREFIX + vmId;
    }

}
