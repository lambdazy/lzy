package ai.lzy.allocator.volume;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.alloc.impl.kuber.KuberUtils;
import ai.lzy.allocator.model.DiskVolumeDescription;
import ai.lzy.allocator.model.NFSVolumeDescription;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.allocator.model.VolumeRequest;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import ai.lzy.allocator.volume.dao.VolumeClaimDao;
import ai.lzy.allocator.volume.dao.VolumeDao;
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
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

import static ai.lzy.model.db.DbHelper.withRetries;

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
    private final VolumeDao volumeDao;
    private final VolumeClaimDao volumeClaimDao;

    public KuberVolumeManager(KuberClientFactory kuberClientFactory, ClusterRegistry clusterRegistry,
                              VolumeDao volumeDao, VolumeClaimDao volumeClaimDao)
    {
        this.kuberClientFactory = kuberClientFactory;
        this.clusterRegistry = clusterRegistry;
        this.volumeDao = volumeDao;
        this.volumeClaimDao = volumeClaimDao;
    }

    @Override
    public Volume createOrGet(String clusterId, VolumeRequest volumeRequest) {
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

        Volume existingVolume = tryGetExistingVolume(clusterId, diskId);
        if (existingVolume != null) {
            return existingVolume;
        }

        var id = UUID.randomUUID().toString();
        final var result = new Volume(id, clusterId, volumeName, resourceName, diskId, diskSize, accessMode,
            storageClass);

        try (var client = kuberClientFactory.build(cluster)) {
            client.persistentVolumes().resource(volume).create();
            LOG.info("Successfully created persistent volume {} for disk={} ", volumeName, diskId);
        } catch (KubernetesClientException e) {
            if (KuberUtils.isResourceAlreadyExist(e)) {
                LOG.warn("Volume {} already exists", result);
            } else {
                LOG.error("Could not create volume {}: {}", result, e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        return trySaveVolume(result);
    }

    private Volume trySaveVolume(Volume result) {
        try {
            withRetries(LOG, () -> volumeDao.create(result, null));
        } catch (Exception e) {
            LOG.error("Could not create volume {} in DB: {}", result, e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return result;
    }

    @Nullable
    private Volume tryGetExistingVolume(String clusterId, String diskId) {
        try {
            var existingVolume = withRetries(LOG, () -> volumeDao.getByDisk(clusterId, diskId, null));
            if (existingVolume != null) {
                LOG.info("Volume {} already exists in DB", existingVolume.name());
            }
            return existingVolume;
        } catch (Exception e) {
            LOG.error("Could not get volume by clusterId={} and diskId={} from DB: {}", clusterId, diskId,
                e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public VolumeClaim createClaim(Volume volume) {
        var cluster = getClusterOrThrow(volume.clusterId());
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

        VolumeClaim existingClaim = tryGetVolumeClaimByVolume(volume);
        if (existingClaim != null) {
            return existingClaim;
        }

        var volumeClaimModel = new VolumeClaim(UUID.randomUUID().toString(), volume.clusterId(), claimName, volume);
        try (var client = kuberClientFactory.build(cluster)) {
            final var claim = client.persistentVolumeClaims().resource(volumeClaim).create();
            final var claimId = claim.getMetadata().getUid();
            LOG.info("Successfully created persistent volume claim name={} claimId={}, accessModes=[{}]",
                claimName, claimId, String.join(", ", claim.getSpec().getAccessModes()));
            return new VolumeClaim(UUID.randomUUID().toString(), volume.clusterId(), claimName, volume);
        } catch (KubernetesClientException e) {
            if (KuberUtils.isResourceAlreadyExist(e)) {
                LOG.warn("Claim {} already exist", volumeClaim);
            } else {
                LOG.error("Could not create resource: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }

        return tryCreateVolumeClaim(volumeClaimModel);
    }

    @Nullable
    private VolumeClaim tryGetVolumeClaimByVolume(Volume volume) {
        try {
            var existingClaim = withRetries(LOG, () -> volumeClaimDao.getByVolumeId(volume.clusterId(),
                volume.id(), null));
            if (existingClaim != null) {
                LOG.info("Claim {} already exists in DB", existingClaim.name());
            }
            return existingClaim;
        } catch (Exception e) {
            LOG.error("Could not get claim by volumeId={} from DB: {}", volume.id(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private VolumeClaim tryCreateVolumeClaim(VolumeClaim volumeClaimModel) {
        try {
            withRetries(LOG, () -> volumeClaimDao.create(volumeClaimModel, null));
        } catch (Exception e) {
            LOG.error("Could not create claim {} in DB: {}", volumeClaimModel, e.getMessage(), e);
            throw new RuntimeException(e);
        }
        return volumeClaimModel;
    }

    @Override
    @Nullable
    public Volume get(String clusterId, String volumeName) {
        try {
            return withRetries(LOG, () -> volumeDao.getByName(clusterId, volumeName, null));
        } catch (Exception e) {
            LOG.error("Could not get volume by clusterId={} and volumeName={} from DB: {}", clusterId, volumeName,
                e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    @Nullable
    public VolumeClaim getClaim(String clusterId, String volumeClaimName) {
        try {
            return volumeClaimDao.getByName(clusterId, volumeClaimName, null);
        } catch (Exception e) {
            LOG.error("Could not get claim by clusterId={} and volumeClaimName={} from DB: {}", clusterId,
                volumeClaimName, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String clusterId, String volumeName) {
        try {
            withRetries(LOG, () -> volumeDao.deleteByName(clusterId, volumeName, null));
        } catch (Exception e) {
            LOG.error("Could not delete volume by clusterId={} and volumeName={} from DB: {}", clusterId, volumeName,
                e.getMessage(), e);
            throw new RuntimeException(e);
        }

        var cluster = getClusterOrThrow(clusterId);
        try (var client = kuberClientFactory.build(cluster)) {
            LOG.info("Deleting persistent volume {}", volumeName);
            client.persistentVolumes().withName(volumeName).delete();
            LOG.info("Persistent volume {} successfully deleted", volumeName);
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.warn("Persistent volume {} not found", volumeName);
            } else {
                LOG.error("Cannot delete persistent volume {}: {}", volumeName, e.getMessage(), e);
            }
        }
    }

    @Override
    public void deleteClaim(String clusterId, String volumeClaimName) {
        try {
            withRetries(LOG, () -> volumeClaimDao.deleteByName(clusterId, volumeClaimName, null));
        } catch (Exception e) {
            LOG.error("Could not delete claim by clusterId={} and volumeClaimName={} from DB: {}", clusterId,
                volumeClaimName, e.getMessage(), e);
            throw new RuntimeException(e);
        }

        var cluster = getClusterOrThrow(clusterId);
        try (var client = kuberClientFactory.build(cluster)) {
            LOG.info("Deleting volume claim {}", volumeClaimName);
            client.persistentVolumeClaims().inNamespace(DEFAULT_NAMESPACE).withName(volumeClaimName).delete();
            LOG.info("Volume claim {} successfully deleted", volumeClaimName);
        } catch (KubernetesClientException e) {
            if (e.getCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                LOG.warn("Persistent volume claim {} not found", volumeClaimName);
            } else {
                LOG.error("Cannot delete persistent volume claim {}: {}", volumeClaimName, e.getMessage(), e);
            }
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
