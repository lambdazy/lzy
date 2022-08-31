package ai.lzy.allocator.volume;

import ai.lzy.allocator.disk.Disk;
import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.exceptions.NotFoundException;
import io.fabric8.kubernetes.api.model.CSIPersistentVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuberVolumeManager implements VolumeManager {
    public static final String REQUESTED_VOLUME_NAME_LABEL = "lzy-requested-volume-name";
    private static final Logger LOG = LoggerFactory.getLogger(KuberVolumeManager.class);
    private static final String CAPACITY_STORAGE_KEY = "storage";
    private static final String KUBER_GB_NAME = "Gi";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String EMPTY_STORAGE_CLASS_NAME = "";
    private final KubernetesClient client;
    private final DiskManager diskManager;

    public KuberVolumeManager(KubernetesClient client, DiskManager diskManager) {
        this.client = client;
        this.diskManager = diskManager;
    }

    public static List<VolumeClaim> allocateVolumes(
        KubernetesClient client, DiskManager diskManager, List<DiskVolumeDescription> volumeRequests
    ) {
        LOG.info("Allocate volume " + volumeRequests.stream().map(Objects::toString).collect(Collectors.joining(", ")));
        final VolumeManager volumeManager = new KuberVolumeManager(client, diskManager);
        return volumeRequests.stream()
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

    public static void freeVolumes(KubernetesClient client, DiskManager diskManager, List<VolumeClaim> volumeClaims) {
        LOG.info("Free volumes " + volumeClaims.stream().map(Objects::toString).collect(Collectors.joining(", ")));
        final VolumeManager volumeManager = new KuberVolumeManager(client, diskManager);
        volumeClaims.forEach(volumeClaim -> {
            volumeManager.deleteClaim(volumeClaim.name());
            volumeManager.delete(volumeClaim.volumeName());
        });
    }

    @Override
    public Volume create(DiskVolumeDescription diskVolumeDescription) throws NotFoundException {
        try {
            final String diskId = diskVolumeDescription.diskId();
            LOG.info("Searching for disk=" + diskId);
            final Disk disk = diskManager.get(diskId);
            if (disk == null) {
                throw new NotFoundException("Disk id=" + diskId + " not found");
            }

            final int diskSize = disk.spec().sizeGb();
            LOG.info("Creating persistent volume for disk=" + diskId);
            final String volumeName = "volume-" + randomSuffix();
            final var accessMode = Volume.AccessMode.READ_WRITE_ONCE;
            final PersistentVolume volume = new PersistentVolumeBuilder()
                .withNewMetadata()
                    .withName(volumeName)
                    .withLabels(Collections.singletonMap(REQUESTED_VOLUME_NAME_LABEL, diskVolumeDescription.name()))
                .endMetadata()
                .withNewSpec()
                .addToCapacity(
                    Collections.singletonMap("storage", new Quantity(diskSize + KUBER_GB_NAME)))
                .withAccessModes(accessMode.asString())
                .withStorageClassName(EMPTY_STORAGE_CLASS_NAME)
                .withPersistentVolumeReclaimPolicy("Retain")
                .withCsi(new CSIPersistentVolumeSourceBuilder()
                    .withDriver("disk-csi-driver.mks.ycloud.io")
                    .withFsType("ext4")
                    .withVolumeHandle(diskId)
                    .build())
                .endSpec()
                .build();

            client.persistentVolumes().resource(volume).create();
            LOG.info("Successfully created persistent volume {} for disk={} ", volumeName, diskId);
            return new Volume(volumeName, diskVolumeDescription.name(), diskId, diskSize, accessMode);
        } catch (KubernetesClientException e) {
            LOG.error("Could not create resource: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private String randomSuffix() {
        return UUID.randomUUID().toString();
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
                .withStorageClassName(EMPTY_STORAGE_CLASS_NAME)
                .withResources(
                    new ResourceRequirementsBuilder()
                        .withRequests(Collections.singletonMap(
                            CAPACITY_STORAGE_KEY, Quantity.parse(volume.sizeGb() + KUBER_GB_NAME)))
                        .build())
                .endSpec()
                .build();

            final PersistentVolumeClaim createdClaim = client.persistentVolumeClaims().resource(volumeClaim).create();
            final String claimId = createdClaim.getMetadata().getUid();
            LOG.info("Successfully created persistent volume claim name={} claimId={}", claimName, claimId);
            return new VolumeClaim(claimName, volume);
        }  catch (KubernetesClientException e) {
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
            assert persistentVolume.getSpec().getCapacity().get(CAPACITY_STORAGE_KEY).getFormat().equals(KUBER_GB_NAME);
            assert accessModes.size() == 1;

            final Volume volume = new Volume(
                volumeName,
                persistentVolume.getMetadata().getLabels().get(REQUESTED_VOLUME_NAME_LABEL),
                persistentVolume.getSpec().getCsi().getVolumeHandle(),
                Integer.parseInt(persistentVolume.getSpec().getCapacity().get(CAPACITY_STORAGE_KEY).getAmount()),
                Volume.AccessMode.fromString(accessModes.get(0))
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
