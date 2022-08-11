package ai.lzy.servant.portal;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.slots.S3StorageOutputSlot;
import ai.lzy.v1.LzyPortalApi.PortalSlotDesc.Ordinary;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.lzy.servant.portal.Portal.*;

final class SnapshotLzySlotsProvider {
    private final Map<String, SnapshotSlot> snapshots = new HashMap<>(); // snapshotId -> in & out slots
    private final Map<String, String> slot2snapshot = new HashMap<>(); // slotName -> snapshotId

    private final ExternalStorage externalStorage = new ExternalStorage();

    public LzySlot createLzySlot(Ordinary snapshotData, SlotInstance slot) throws CreatingLzySlotException {
        LzySlot lzySlot;
        if (snapshotData.hasLocalId()) {
            var snapshotId = snapshotData.getLocalId();

            var prevSnapshotId = slot2snapshot.putIfAbsent(slot.name(), snapshotId);
            if (prevSnapshotId != null) {
                throw new CreatingLzySlotException("Slot '" + slot.name()
                    + "' already associated with " + "snapshot '" + prevSnapshotId + "'");
            }
            switch (slot.spec().direction()) {
                case INPUT -> {
                    try {
                        var ss = new SnapshotSlot(snapshotId);
                        var prev = snapshots.put(snapshotId, ss);
                        if (prev != null) {
                            throw new CreatingLzySlotException("Snapshot '" + snapshotId + "' already exists.");
                        }

                        lzySlot = ss.setInputSlot(slot);
                    } catch (IOException e) {
                        throw new CreatingLzySlotException("Error file configuring snapshot storage: "
                            + e.getMessage());
                    }
                }

                case OUTPUT -> {
                    var ss = snapshots.get(snapshotId);
                    if (ss == null) {
                        throw new CreatingLzySlotException("Attempt to open output snapshot " + snapshotId
                            + " slot, while input is not set yet");
                    }
                    if (ss.getOutputSlot(slot.name()) != null) {
                        throw new CreatingLzySlotException("Attempt to open output snapshot " + snapshotId
                            + " slot, while input is not set yet");
                    }

                    lzySlot = ss.addOutputSlot(slot);
                }

                default -> {
                    throw new CreatingLzySlotException("Unknown slot direction " + slot.spec().direction());
                }
            }
        } else {
            var s3SnapshotData = snapshotData.getS3Coords();
            String key = s3SnapshotData.getKey();
            String bucket = s3SnapshotData.getBucket();

            ExternalStorage.S3RepositoryProvider clientProvider = switch (s3SnapshotData.getEndpointCase()) {
                case AMAZON -> ExternalStorage.AmazonS3Key.of(s3SnapshotData.getAmazon().getEndpoint(),
                    s3SnapshotData.getAmazon().getAccessToken(),
                    s3SnapshotData.getAmazon().getSecretToken());
                case AZURE -> ExternalStorage.AzureS3Key.of(s3SnapshotData.getAzure().getConnectionString());
                default -> null;
            };
            if (clientProvider == null) {
                throw new CreatingLzySlotException("Unknown s3 endpoint type " + s3SnapshotData.getEndpointCase());
            }

            lzySlot = switch (slot.spec().direction()) {
                case INPUT -> externalStorage.createSlotSnapshot(slot, key, bucket, clientProvider);
                case OUTPUT -> {
                    S3StorageOutputSlot slot1 = externalStorage.readSlotSnapshot(slot, key,
                        bucket, clientProvider);
                    slot1.open();
                    yield slot1;
                }
            };
            if (lzySlot == null) {
                throw new CreatingLzySlotException("Unknown slot direction " + slot.spec().direction());
            }
        }
        return lzySlot;
    }

    public void removeLzyInputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(slot2snapshot.get(slotName));
        if (ss != null) {
            ss.removeInputSlot(slotName);
        } else {
            externalStorage.removeInputSlot(slotName);
        }
    }

    public void removeLzyOutputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(slot2snapshot.get(slotName));
        if (ss != null) {
            ss.removeOutputSlot(slotName);
        } else {
            externalStorage.removeOutputSlot(slotName);
        }
    }

    public Collection<? extends LzyInputSlot> lzyInputSlots() {
        return Stream.concat(externalStorage.getInputSlots().stream(),
            snapshots.values().stream().map(SnapshotSlot::getInputSlot).filter(Objects::nonNull)).toList();
    }

    public Collection<? extends LzyOutputSlot> lzyOutputSlots() {
        return Stream.concat(externalStorage.getOutputSlots().stream(),
            snapshots.values().stream().flatMap(slot -> slot.getOutputSlots().stream())).toList();
    }

    public LzyInputSlot lzyInputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(slot2snapshot.get(slotName));
        return (ss != null) ? ss.getInputSlot() : externalStorage.getInputSlot(slotName);
    }

    public LzyOutputSlot lzyOutputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(slot2snapshot.get(slotName));
        return (ss != null) ? ss.getOutputSlot(slotName) : externalStorage.getOutputSlot(slotName);
    }
}
