package ai.lzy.allocator.volume;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.model.DiskVolumeDescription;
import ai.lzy.allocator.model.NFSVolumeDescription;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.CSIPersistentVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

@Singleton
public class KuberVolumeManager implements VolumeManager {
    public static final String REQUESTED_VOLUME_NAME_LABEL = "lzy-requested-volume-name";
    public static final String YCLOUD_DISK_DRIVER = "disk-csi-driver.mks.ycloud.io";

    public static final String NFS_DRIVER = "nfs.csi.k8s.io";
    public static final String KUBER_GB_NAME = "Gi";
    public static final String VOLUME_CAPACITY_STORAGE_KEY = "storage";

    private static final Logger LOG = LogManager.getLogger(KuberVolumeManager.class);
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String EMPTY_STORAGE_CLASS_NAME = "";
    private static final String NFS_STORAGE_CLASS_NAME = "nfs-csi";

    private final KuberClientFactory kuberClientFactory;
    private final ClusterRegistry clusterRegistry;

    public KuberVolumeManager(KuberClientFactory kuberClientFactory, ClusterRegistry clusterRegistry) {
        this.kuberClientFactory = kuberClientFactory;
        this.clusterRegistry = clusterRegistry;
    }

    @Override
    public Volume create(String clusterId, VolumeRequest volumeRequest) {
        final String diskId;
        final int diskSize;
        final String resourceName;
        final Volume.AccessMode accessMode;
        final PersistentVolume volume;
        final String storageClass;

        var cluster = getClusterOrThrow(clusterId);
        final String volumeName = volumeRequest.volumeId();

        if (volumeRequest.volumeDescription() instanceof DiskVolumeDescription diskVolumeDescription) {
            diskId = diskVolumeDescription.diskId();
            diskSize = diskVolumeDescription.sizeGb();

            LOG.info("Creating persistent volume '{}' for disk {} of size {}Gi", volumeName, diskId, diskSize >> 30);

            accessMode = Volume.AccessMode.READ_WRITE_ONCE;
            resourceName = diskVolumeDescription.name();
            storageClass = EMPTY_STORAGE_CLASS_NAME;
            volume = new PersistentVolumeBuilder()
                .withNewMetadata()
                    .withName(volumeName)
                    .withLabels(Map.of(REQUESTED_VOLUME_NAME_LABEL, diskVolumeDescription.name()))
                .endMetadata()
                .withNewSpec()
                    .addToCapacity(Map.of("storage", new Quantity(diskSize + KUBER_GB_NAME)))
                    .withAccessModes(accessMode.asString())
                    .withStorageClassName(storageClass)
                    .withPersistentVolumeReclaimPolicy("Retain")
                    .withCsi(new CSIPersistentVolumeSourceBuilder()
                        .withDriver(YCLOUD_DISK_DRIVER)
                        .withFsType("ext4")
                        .withVolumeHandle(diskId)
                        .build())
                .endSpec()
                .build();
        } else if (volumeRequest.volumeDescription() instanceof NFSVolumeDescription nfsVolumeDescription) {
            diskId = volumeName; // NFS doesn't have real diskId, but it is required field for CSI
            diskSize = 1; // it's needed for volume <-> volume claim matching

            LOG.info("Creating persistent NFS volume for disk=" + volumeName);

            accessMode = nfsVolumeDescription.readOnly()
                ? Volume.AccessMode.READ_ONLY_MANY
                : Volume.AccessMode.READ_WRITE_MANY;
            resourceName = nfsVolumeDescription.name();
            storageClass = NFS_STORAGE_CLASS_NAME;
            volume = new PersistentVolumeBuilder()
                .withNewMetadata()
                    .withName(volumeName)
                    .withLabels(Map.of(REQUESTED_VOLUME_NAME_LABEL, nfsVolumeDescription.name()))
                .endMetadata()
                .withNewSpec()
                    .addToCapacity(Map.of("storage", new Quantity(diskSize + KUBER_GB_NAME)))
                    .withAccessModes(accessMode.asString())
                    .withStorageClassName(storageClass)
                    .withMountOptions(nfsVolumeDescription.mountOptions())
                    .withCsi(new CSIPersistentVolumeSourceBuilder()
                        .withDriver(NFS_DRIVER)
                        .withVolumeHandle(diskId)
                        .withReadOnly(nfsVolumeDescription.readOnly())
                        .withVolumeAttributes(Map.of(
                            "server", nfsVolumeDescription.server(),
                            "share", nfsVolumeDescription.share()))
                        .build())
                .endSpec()
                .build();
        } else {
            LOG.error("Unknown Resource Volume:: {}", volumeRequest.volumeDescription());
            throw new RuntimeException("Unknown Resource Volume " + volumeRequest.volumeDescription().name());
        }

        final var result = new Volume(volumeName, resourceName, diskId, diskSize, accessMode, storageClass);

        try (var client = kuberClientFactory.build(cluster)) {
            client.persistentVolumes().resource(volume).create();
            LOG.info("Successfully created persistent volume {} for disk={} ", volumeName, diskId);
            return result;
        } catch (KubernetesClientException e) {
            if (KuberUtils.isResourceAlreadyExist(e)) {
                LOG.warn("Volume {} already exists", result);
                return result;
            }

            LOG.error("Could not create volume {}: {}", result, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public VolumeClaim createClaim(String clusterId, Volume volume) {
        var cluster = getClusterOrThrow(clusterId);
        final var claimName = "claim-" + volume.name();

        LOG.info("Creating persistent volume claim {} for volume={}", claimName, volume);

        final PersistentVolumeClaim volumeClaim = new PersistentVolumeClaimBuilder()
            .withNewMetadata()
                .withName(claimName)
                .withNamespace(DEFAULT_NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withAccessModes(volume.accessMode().asString())
                .withVolumeName(volume.name())
                .withStorageClassName(volume.storageClass())
                .withResources(
                    new ResourceRequirementsBuilder()
                        .withRequests(Map.of(
                            VOLUME_CAPACITY_STORAGE_KEY, Quantity.parse(volume.sizeGb() + KUBER_GB_NAME)))
                        .build())
            .endSpec()
            .build();

        var result = new VolumeClaim(claimName, volume);
        try (var client = kuberClientFactory.build(cluster)) {
            final var claim = client.persistentVolumeClaims().resource(volumeClaim).create();
            final var claimId = claim.getMetadata().getUid();
            LOG.info("Successfully created persistent volume claim name={} claimId={}, accessModes=[{}]",
                claimName, claimId, String.join(", ", claim.getSpec().getAccessModes()));
            return result;
        } catch (KubernetesClientException e) {
            if (KuberUtils.isResourceAlreadyExist(e)) {
                LOG.warn("Claim {} already exist", volumeClaim);
                return result;
            }

            LOG.error("Could not create resource: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Override
    @Nullable
    public Volume get(String clusterId, String volumeName) {
        var cluster = getClusterOrThrow(clusterId);
        try (var client = kuberClientFactory.build(cluster)) {
            LOG.info("Trying to find volume with name={}", volumeName);
            final PersistentVolume persistentVolume = client.persistentVolumes().withName(volumeName).get();
            if (persistentVolume == null) {
                return null;
            }

            final List<String> accessModes = persistentVolume.getSpec().getAccessModes();
            assert persistentVolume.getSpec().getCapacity()
                    .get(VOLUME_CAPACITY_STORAGE_KEY).getFormat().equals(KUBER_GB_NAME);
            assert accessModes.size() == 1;

            final Volume volume = new Volume(
                volumeName,
                persistentVolume.getMetadata().getLabels().get(REQUESTED_VOLUME_NAME_LABEL),
                persistentVolume.getSpec().getCsi().getVolumeHandle(),
                Integer.parseInt(
                    persistentVolume.getSpec().getCapacity().get(VOLUME_CAPACITY_STORAGE_KEY).getAmount()),
                Volume.AccessMode.fromString(accessModes.get(0)),
                persistentVolume.getSpec().getStorageClassName());

            LOG.info("Found volume={}", volume);
            return volume;
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.error("Not found volume with name={}", volumeName);
                return null;
            }
            throw e;
        }
    }

    @Override
    @Nullable
    public VolumeClaim getClaim(String clusterId, String volumeClaimName) {
        var cluster = getClusterOrThrow(clusterId);
        try (var client = kuberClientFactory.build(cluster)) {
            LOG.info("Trying to find volumeClaim with name={}", volumeClaimName);
            final var pvc = client.persistentVolumeClaims()
                    .inNamespace(DEFAULT_NAMESPACE).withName(volumeClaimName).get();
            if (pvc == null) {
                return null;
            }

            final List<String> accessModes = pvc.getSpec().getAccessModes();
            assert accessModes.size() == 1;
            final VolumeClaim volumeClaim = new VolumeClaim(volumeClaimName, get(clusterId,
                pvc.getSpec().getVolumeName()));
            LOG.info("Found {}", volumeClaim);
            return volumeClaim;
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.error("Not found volumeClaim with name={}", volumeClaimName);
                return null;
            }
            throw e;
        }
    }

    @Override
    public void delete(String clusterId, String volumeName) {
        var cluster = getClusterOrThrow(clusterId);
        try (var client = kuberClientFactory.build(cluster)) {
            LOG.info("Deleting persistent volume {}", volumeName);
            client.persistentVolumes().withName(volumeName).delete();
            LOG.info("Persistent volume {} successfully deleted", volumeName);
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.warn("Persistent volume {} not found", volumeName);
                return;
            }
            LOG.error("Cannot delete persistent volume {}: {}", volumeName, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteClaim(String clusterId, String volumeClaimName) {
        var cluster = getClusterOrThrow(clusterId);
        try (var client = kuberClientFactory.build(cluster)) {
            LOG.info("Deleting volume claim {}", volumeClaimName);
            client.persistentVolumeClaims().inNamespace(DEFAULT_NAMESPACE).withName(volumeClaimName).delete();
            LOG.info("Volume claim {} successfully deleted", volumeClaimName);
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.warn("Persistent volume claim {} not found", volumeClaimName);
                return;
            }
            LOG.error("Cannot delete persistent volume claim {}: {}", volumeClaimName, e.getMessage(), e);
            throw e;
        }
    }

    @NotNull
    private ClusterRegistry.ClusterDescription getClusterOrThrow(String clusterId) {
        var cluster = clusterRegistry.getCluster(clusterId);
        if (cluster == null) {
            throw new IllegalArgumentException("Cluster " + clusterId + " not found");
        }
        return cluster;
    }
}
