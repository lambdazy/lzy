package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.exceptions.SnapshotNotFound;
import ai.lzy.portal.exceptions.SnapshotUniquenessException;
import ai.lzy.v1.portal.LzyPortal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class SnapshotSlots {
    private static final Logger LOG = LogManager.getLogger(SnapshotSlots.class);

    private final Map<URI, SnapshotEntry> snapshots = new HashMap<>(); // snapshot storage uri -> snapshot entry
    private final Map<String, URI> name2uri = new HashMap<>(); // slot name -> snapshot storage uri

    private final ExecutorService downloadUploadExecutor;

    public SnapshotSlots(ExecutorService downloadUploadExecutor) {
        this.downloadUploadExecutor = downloadUploadExecutor;
    }

    public synchronized LzySlot createSlot(LzyPortal.PortalSlotDesc.Snapshot snapshotData, SlotInstance slotData)
        throws CreateSlotException
    {
        URI existsUri = name2uri.get(slotData.name());
        if (existsUri != null) {
            LOG.error("Slot already associated with snapshot data: { slotName: {}, dataUri: {} }",
                slotData.name(), existsUri);
            throw new SnapshotUniquenessException("Slot already associated with snapshot data");
        }

        var storageClient = StorageClients.provider(snapshotData.getStorageConfig()).get(downloadUploadExecutor);
        var uri = URI.create(snapshotData.getStorageConfig().getUri());

        boolean alreadyHasData;
        try {
            alreadyHasData = storageClient.blobExists(uri);
        } catch (Exception e) {
            LOG.error("Unable to connect to storage while checking existence: {}", e.getMessage(), e);
            throw new CreateSlotException(e);
        }

        LzySlot lzySlot = switch (slotData.spec().direction()) {
            case INPUT -> {
                if (snapshots.containsKey(uri) || alreadyHasData) {
                    throw new SnapshotUniquenessException("Storage already has snapshot data by uri: " + uri);
                }

                SnapshotEntry snapshot = getOrCreateSnapshot(uri);
                SnapshotInputSlot inputSlot;
                try {
                    inputSlot = new SnapshotInputSlot(slotData, snapshot, storageClient, null);
                } catch (IOException e) {
                    throw new CreateSlotException(e);
                }
                snapshot.setInputSlot(inputSlot);

                yield inputSlot;
            }
            case OUTPUT -> {
                if (!snapshots.containsKey(uri) && !alreadyHasData) {
                    throw new SnapshotNotFound("Cannot found snapshot data by uri: " + uri);
                }

                SnapshotEntry snapshot = getOrCreateSnapshot(uri);
                var outputSlot = new SnapshotOutputSlot(slotData, snapshot, storageClient);
                snapshot.addOutputSlot(outputSlot);

                yield outputSlot;
            }
        };

        name2uri.put(slotData.name(), uri);

        return lzySlot;
    }

    public Collection<? extends LzyInputSlot> getInputSlots() {
        return snapshots.values().stream().map(SnapshotEntry::getInputSlot).filter(Objects::nonNull).toList();
    }

    public Collection<? extends LzyOutputSlot> getOutputSlots() {
        return snapshots.values().stream().flatMap(slot -> slot.getOutputSlots().stream()).toList();
    }

    public LzyInputSlot getInputSlot(String slotName) {
        SnapshotEntry snapshot = snapshots.get(name2uri.get(slotName));
        return Objects.nonNull(snapshot) ? snapshot.getInputSlot() : null;
    }

    public LzyOutputSlot getOutputSlot(String slotName) {
        SnapshotEntry snapshot = snapshots.get(name2uri.get(slotName));
        return Objects.nonNull(snapshot) ? snapshot.getOutputSlot(slotName) : null;
    }

    public boolean removeInputSlot(String slotName) {
        SnapshotEntry snapshot = snapshots.get(name2uri.get(slotName));
        return snapshot != null && snapshot.removeInputSlot(slotName);
    }

    public boolean removeOutputSlot(String slotName) {
        SnapshotEntry snapshot = snapshots.get(name2uri.get(slotName));
        return snapshot != null && snapshot.removeOutputSlot(slotName);
    }

    private SnapshotEntry getOrCreateSnapshot(URI storageUri) throws CreateSlotException {
        try {
            return snapshots.computeIfAbsent(storageUri,
                uri -> {
                    try {
                        return new SnapshotEntry(uri);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        } catch (Exception e) {
            throw new CreateSlotException(e.getCause());
        }
    }
}
