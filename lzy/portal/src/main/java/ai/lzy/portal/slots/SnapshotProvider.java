package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.exceptions.SnapshotNotFound;
import ai.lzy.portal.exceptions.SnapshotUniquenessException;
import ai.lzy.storage.StorageClient;
import ai.lzy.storage.azure.AzureClientWithTransmitter;
import ai.lzy.storage.s3.S3ClientWithTransmitter;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.portal.LzyPortal;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

@Singleton
public class SnapshotProvider {
    private static final Logger LOG = LogManager.getLogger(SnapshotProvider.class);

    private final Map<String, Snapshot> snapshots = new HashMap<>(); // snapshot id -> snapshot slot
    private final Map<String, String> name2id = new HashMap<>(); // slot name -> snapshot id
    private final ExecutorService workers;

    public SnapshotProvider(@Named("PortalServiceExecutor") ExecutorService workers) {
        this.workers = workers;
    }

    public synchronized LzySlot createSlot(LzyPortal.PortalSlotDesc.Snapshot snapshotData, SlotInstance instance)
        throws CreateSlotException
    {
        URI uri = URI.create(snapshotData.getStorageConfig().getUri());
        var snapshotId = snapshotData.getStorageConfig().getUri();
        var previousSnapshotId = name2id.get(instance.name());
        if (Objects.nonNull(previousSnapshotId)) {
            throw new SnapshotUniquenessException("Slot '" + instance.name() + "' already associated with "
                + "snapshot '" + previousSnapshotId + "'");
        }

        StorageClient storageClient = getS3RepositoryForSnapshots(snapshotData.getStorageConfig());

        boolean s3ContainsSnapshot;
        try {
            s3ContainsSnapshot = storageClient.blobExists(uri); // request to s3
        } catch (Exception e) {
            LOG.error("Unable to connect to S3 storage: {}", e.getMessage(), e);
            throw new CreateSlotException(e);
        }

        LzySlot lzySlot = switch (instance.spec().direction()) {
            case INPUT -> {
                if (snapshots.containsKey(snapshotId) || s3ContainsSnapshot) {
                    throw new SnapshotUniquenessException("Snapshot with id '" + snapshotId +
                        "' already associated with data");
                }

                yield getOrCreateSnapshot(storageClient, snapshotId, uri).setInputSlot(instance, null);
            }
            case OUTPUT -> {
                if (!snapshots.containsKey(snapshotId) && !s3ContainsSnapshot) {
                    throw new SnapshotNotFound("Snapshot with id '" + snapshotId + "' not found");
                }

                yield getOrCreateSnapshot(storageClient, snapshotId, uri).addOutputSlot(instance);
            }
        };

        name2id.put(instance.name(), snapshotId);

        return lzySlot;
    }

    private Snapshot getOrCreateSnapshot(StorageClient storageClient, String snapshotId,
                                         URI uri) throws CreateSlotException
    {
        try {
            return snapshots.computeIfAbsent(snapshotId,
                id -> {
                    try {
                        return new S3Snapshot(id, uri, storageClient);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        } catch (Exception e) {
            throw new CreateSlotException(e.getMessage());
        }
    }

    private StorageClient getS3RepositoryForSnapshots(LMST.StorageConfig storageConfig) {
        if (storageConfig.hasAzure()) {
            return new AzureClientWithTransmitter(storageConfig.getAzure().getConnectionString(), workers);
        } else {
            assert storageConfig.hasS3();
            return new S3ClientWithTransmitter(storageConfig.getS3().getEndpoint(),
                storageConfig.getS3().getAccessToken(), storageConfig.getS3().getSecretToken(), workers);
        }
    }

    public boolean removeInputSlot(String slotName) {
        Snapshot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) && ss.removeInputSlot(slotName);
    }

    public boolean removeOutputSlot(String slotName) {
        Snapshot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) && ss.removeOutputSlot(slotName);
    }

    public Collection<? extends LzyInputSlot> getInputSlots() {
        return snapshots.values().stream().map(Snapshot::getInputSlot).filter(Objects::nonNull).toList();
    }

    public Collection<? extends LzyOutputSlot> getOutputSlots() {
        return snapshots.values().stream().flatMap(slot -> slot.getOutputSlots().stream()).toList();
    }

    public LzyInputSlot getInputSlot(String slotName) {
        Snapshot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) ? ss.getInputSlot() : null;
    }

    public LzyOutputSlot getOutputSlot(String slotName) {
        Snapshot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) ? ss.getOutputSlot(slotName) : null;
    }
}
