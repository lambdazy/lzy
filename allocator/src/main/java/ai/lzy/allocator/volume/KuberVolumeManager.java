package ai.lzy.allocator.volume;

import ai.lzy.allocator.disk.exceptions.NotFoundException;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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

    private final KubernetesClient client;

    public KuberVolumeManager(KubernetesClient client) {
        this.client = client;
    }

    public static List<VolumeClaim> allocateVolumes(
            KubernetesClient client, List<VolumeRequest.ResourceVolumeDescription> volumesRequest)
    {
        LOG.info("Allocate volume " + volumesRequest.stream().map(Objects::toString).collect(Collectors.joining(", ")));
        final VolumeManager volumeManager = new KuberVolumeManager(client);
        return volumesRequest.stream()
                .map(volumeRequest -> {
                    final Volume volume;
                    try {
                        volume = volumeManager.create(volumeRequest);
                    } catch (NotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    return volumeManager.createClaim(volume);
                }).toList();
    }

    public static void freeVolumes(KubernetesClient client, List<VolumeClaim> volumeClaims) {
        LOG.info("Free volumes " + volumeClaims.stream().map(Objects::toString).collect(Collectors.joining(", ")));
        final VolumeManager volumeManager = new KuberVolumeManager(client);
        volumeClaims.forEach(volumeClaim -> {
            volumeManager.deleteClaim(volumeClaim.name());
            volumeManager.delete(volumeClaim.volumeName());
        });
    }

    @Override
    public Volume create(VolumeRequest.ResourceVolumeDescription resourceVolumeDescription) throws NotFoundException {
        final String diskId;
        final int diskSize;
        final String volumeName;
        final String resourceName;
        final Volume.AccessMode accessMode;
        final PersistentVolume volume;
        final String storageClass;
        if (resourceVolumeDescription instanceof DiskVolumeDescription diskVolumeDescription) {
            diskId = diskVolumeDescription.diskId();
            diskSize = diskVolumeDescription.sizeGb();
            LOG.info("Creating persistent volume for disk=" + diskId);
            volumeName = "volume-" + randomSuffix();
            accessMode = Volume.AccessMode.READ_WRITE_ONCE;
            resourceName = diskVolumeDescription.name();
            storageClass = EMPTY_STORAGE_CLASS_NAME;
            volume = new PersistentVolumeBuilder()
                    .withNewMetadata()
                    .withName(volumeName)
                    .withLabels(Collections.singletonMap(REQUESTED_VOLUME_NAME_LABEL, diskVolumeDescription.name()))
                    .endMetadata()
                    .withNewSpec()
                    .addToCapacity(
                            Collections.singletonMap("storage", new Quantity(diskSize + KUBER_GB_NAME)))
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
        } else if (resourceVolumeDescription instanceof NFSVolumeDescription nfsVolumeDescription) {
            diskId = nfsVolumeDescription.volumeId();
            diskSize = nfsVolumeDescription.capacity();
            LOG.info("Creating persistent NFS volume for disk=" + diskId);
            volumeName = "nfs-volume-" + randomSuffix();
            accessMode = Volume.AccessMode.READ_WRITE_MANY;
            resourceName = nfsVolumeDescription.name();
            storageClass = NFS_STORAGE_CLASS_NAME;
            volume = new PersistentVolumeBuilder()
                    .withNewMetadata()
                    .withName(volumeName)
                    .withLabels(Collections.singletonMap(REQUESTED_VOLUME_NAME_LABEL, nfsVolumeDescription.name()))
                    .endMetadata()
                    .withNewSpec()
                    .addToCapacity(
                            Collections.singletonMap("storage", new Quantity(diskSize + KUBER_GB_NAME)))
                    .withAccessModes(accessMode.asString())
                    .withStorageClassName(storageClass)
                    .withMountOptions(nfsVolumeDescription.mountOptions())
                    .withCsi(new CSIPersistentVolumeSourceBuilder()
                            .withDriver(NFS_DRIVER)
                            .withVolumeHandle(diskId)
                            .withVolumeAttributes(Map.of(
                                    "server", nfsVolumeDescription.server(),
                                    "share", nfsVolumeDescription.share()
                            ))
                            .build())
                    .endSpec()
                    .build();

        } else {
            LOG.error("Unknown Resource Volume:: {}", resourceVolumeDescription);
            throw new RuntimeException("Unknown Resource Volume " + resourceVolumeDescription.name());
        }
        try {
            client.persistentVolumes().resource(volume).create();
            LOG.info("Successfully created persistent volume {} for disk={} ", volumeName, diskId);
            return new Volume(volumeName, resourceName, diskId, diskSize, accessMode, storageClass);
        } catch (KubernetesClientException e) {
            LOG.error("Could not create resource: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private String randomSuffix() {
        return UUID.randomUUID().toString();
    }

    private boolean pvcExists(String volumeName) {
        return client.persistentVolumes().list().getItems().stream()
                .anyMatch(v -> v.getMetadata().getLabels().get(REQUESTED_VOLUME_NAME_LABEL).equals(volumeName));
    }

    @Override
    public VolumeClaim createClaim(Volume volume) {
        try {
            LOG.info("Creating persistent volume claim for volume={}", volume);

            final String claimName = "volume-claim-" + randomSuffix();
            final Volume.AccessMode accessMode = Volume.AccessMode.READ_WRITE_ONCE;
            final PersistentVolumeClaim volumeClaim = new PersistentVolumeClaimBuilder()
                    .withNewMetadata().withName(claimName).withNamespace(DEFAULT_NAMESPACE).endMetadata()
                    .withNewSpec()
                    .withAccessModes(accessMode.asString())
                    .withVolumeName(volume.name())
                    .withStorageClassName(volume.storageClass())
                    .withResources(
                            new ResourceRequirementsBuilder()
                                    .withRequests(Collections.singletonMap(
                                            VOLUME_CAPACITY_STORAGE_KEY,
                                            Quantity.parse(volume.sizeGb() + KUBER_GB_NAME)))
                                    .build())
                    .endSpec()
                    .build();

            final PersistentVolumeClaim createdClaim = client.persistentVolumeClaims().resource(volumeClaim).create();
            final String claimId = createdClaim.getMetadata().getUid();
            LOG.info("Successfully created persistent volume claim name={} claimId={}", claimName, claimId);
            return new VolumeClaim(claimName, volume);
        } catch (KubernetesClientException e) {
            LOG.error("Could not create resource: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    @Nullable
    public Volume get(String volumeName) {
        try {
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
                            persistentVolume.getSpec().getCapacity().get(VOLUME_CAPACITY_STORAGE_KEY).getAmount()
                    ),
                    Volume.AccessMode.fromString(accessModes.get(0)),
                    persistentVolume.getSpec().getStorageClassName()
            );
            LOG.info("Found volume={}", volume);
            return volume;
        } catch (KubernetesClientException e) {
            LOG.error("Not found volume with name={}", volumeName);
            return null;
        }
    }

    @Override
    @Nullable
    public VolumeClaim getClaim(String volumeClaimName) {
        try {
            LOG.info("Trying to find volumeClaim with name={}", volumeClaimName);
            final var pvc = client.persistentVolumeClaims()
                    .inNamespace(DEFAULT_NAMESPACE).withName(volumeClaimName).get();
            if (pvc == null) {
                return null;
            }

            final List<String> accessModes = pvc.getSpec().getAccessModes();
            assert accessModes.size() == 1;
            final VolumeClaim volumeClaim = new VolumeClaim(volumeClaimName, get(pvc.getSpec().getVolumeName()));
            LOG.info("Found {}", volumeClaim);
            return volumeClaim;
        } catch (KubernetesClientException e) {
            LOG.error("Not found volumeClaim with name={}", volumeClaimName);
            return null;
        }
    }

    @Override
    public void delete(String volumeName) {
        try {
            LOG.info("Deleting persistent volume {}", volumeName);
            client.persistentVolumes().withName(volumeName).delete();
            LOG.info("Persistent volume {} successfully deleted", volumeName);
        } catch (KubernetesClientException e) {
            LOG.error("Could not delete resource: {}", e.getMessage(), e);
        }
    }

    @Override
    public void deleteClaim(String volumeClaimName) {
        try {
            LOG.info("Deleting volume claim {}", volumeClaimName);
            client.persistentVolumeClaims().inNamespace(DEFAULT_NAMESPACE).withName(volumeClaimName).delete();
            LOG.info("Volume claim {} successfully deleted", volumeClaimName);
        } catch (KubernetesClientException e) {
            LOG.error("Could not delete resource: {}", e.getMessage(), e);
        }
    }
}
