package ai.lzy.allocator.volume;

import ai.lzy.allocator.alloc.impl.kuber.KuberClientFactory;
import ai.lzy.allocator.model.Volume;
import ai.lzy.allocator.model.*;
import ai.lzy.allocator.util.KuberUtils;
import ai.lzy.allocator.vmpool.ClusterRegistry;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Singleton
public class KuberVolumeManager implements VolumeManager {
    public static final String REQUESTED_VOLUME_NAME_LABEL = "lzy-requested-volume-name";
    public static final String KUBER_GB_NAME = "Gi";
    public static final String VOLUME_CAPACITY_STORAGE_KEY = "storage";

    private static final Logger LOG = LogManager.getLogger(KuberVolumeManager.class);
    private static final String DEFAULT_NAMESPACE = "default";

    private final KuberClientFactory kuberClientFactory;
    private final ClusterRegistry clusterRegistry;
    private final StorageProvider storageProvider;

    public KuberVolumeManager(KuberClientFactory kuberClientFactory, ClusterRegistry clusterRegistry,
                              StorageProvider storageProvider)
    {
        this.kuberClientFactory = kuberClientFactory;
        this.clusterRegistry = clusterRegistry;
        this.storageProvider = storageProvider;
    }

    @Override
    public Volume create(String clusterId, VolumeRequest volumeRequest) throws RetryLaterException {
        final String diskId;
        final int diskSize;
        final String resourceName;
        final Volume.AccessMode accessMode;
        final PersistentVolume volume;
        final String storageClass;
        final String fsType;

        var cluster = getClusterOrThrow(clusterId);
        String volumeName = volumeRequest.volumeId();

        if (volumeRequest.volumeDescription() instanceof DiskVolumeDescription diskVolumeDescription) {
            diskId = diskVolumeDescription.diskId();
            diskSize = diskVolumeDescription.sizeGb();

            accessMode = Objects.requireNonNullElse(diskVolumeDescription.accessMode(),
                Volume.AccessMode.READ_WRITE_ONCE);
            resourceName = diskVolumeDescription.name();
            storageClass = storageProvider.resolveDiskStorageClass(diskVolumeDescription.storageClass());
            fsType = storageProvider.resolveDiskFsType(diskVolumeDescription.fsType());
            var readOnly = accessMode == Volume.AccessMode.READ_ONLY_MANY;
            volumeName = generateDiskVolumeName(diskId);

            LOG.info("Creating persistent volume '{}' with description {}", volumeName, diskVolumeDescription);
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
                        .withDriver(storageProvider.diskDriverName())
                        .withFsType("ext4")
                        .withVolumeHandle(diskId)
                        .withReadOnly(readOnly)
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
            storageClass = storageProvider.nfsStorageClass();
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
                        .withDriver(storageProvider.nfsDriverName())
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
            try {
                createPV(client, volume);
                LOG.info("Successfully created persistent volume {} for disk={} ", volumeName, diskId);
                return result;
            } catch (KubernetesClientException e) {
                if (KuberUtils.isResourceAlreadyExist(e)) {
                    LOG.warn("Volume {} already exists", result);
                    return tryRecreatePV(volume, result, client);
                }

                LOG.error("Could not create volume {}: {}", result, e.getMessage(), e);
                throw e;
            }
        }
    }

    private Volume tryRecreatePV(PersistentVolume volume, Volume result, KubernetesClient client)
        throws RetryLaterException
    {
        PersistentVolume existingVolume;
        try {
            existingVolume = client.persistentVolumes().withName(result.name()).get();
        } catch (StatusRuntimeException e) {
            LOG.error("Could not create volume {}: {}", result, e.getMessage(), e);
            throw e;
        }

        if (existingVolume == null) {
            createPV(client, volume);
            LOG.info("Successfully created persistent volume {} for disk={} ", result.name(), result.diskId());
            return result;
        }

        var phase = PersistentVolumePhase.fromString(existingVolume.getStatus().getPhase());
        return switch (phase) {
            case AVAILABLE, BOUND -> {
                LOG.info("Volume {} already exists with active status {}",
                    result, existingVolume.getStatus().getPhase());
                yield result;
            }

            case RELEASED -> {
                LOG.info("Volume {} is terminating, wait...", result);
                throw new RetryLaterException("Volume %s is terminating".formatted(result), Duration.ofMillis(300));
            }

            case FAILED -> {
                LOG.error("Volume {} is in FAILED state {}", result, existingVolume.getStatus());
                throw new RuntimeException("Cannot create already failed volume %s for disk %s"
                    .formatted(result.name(), result.diskId()));
            }
        };
    }

    private PersistentVolume createPV(KubernetesClient client, PersistentVolume volume) {
        return client.persistentVolumes().resource(volume).create();
    }

    private static String generateDiskVolumeName(String diskId) {
        return "vm-volume-" + diskId;
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
            if (KuberUtils.isResourceNotFound(e)) {
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
            if (KuberUtils.isResourceNotFound(e)) {
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
        } catch (KubernetesClientException e) {
            if (KuberUtils.isResourceNotFound(e)) {
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
        } catch (KubernetesClientException e) {
            if (KuberUtils.isResourceNotFound(e)) {
                LOG.warn("Persistent volume claim {} not found", volumeClaimName);
                return;
            }
            LOG.error("Cannot delete persistent volume claim {}: {}", volumeClaimName, e.getMessage(), e);
            throw e;
        }
    }

    @Nonnull
    private ClusterRegistry.ClusterDescription getClusterOrThrow(String clusterId) {
        var cluster = clusterRegistry.getCluster(clusterId);
        if (cluster == null) {
            throw new IllegalArgumentException("Cluster " + clusterId + " not found");
        }
        return cluster;
    }
}
