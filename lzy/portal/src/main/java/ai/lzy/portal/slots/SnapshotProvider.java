package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.exceptions.SnapshotNotFound;
import ai.lzy.portal.exceptions.SnapshotUniquenessException;
import ai.lzy.portal.storage.ByteStringStreamConverter;
import ai.lzy.portal.storage.Repository;
import ai.lzy.portal.storage.StorageRepositories;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.portal.LzyPortal;
import com.amazonaws.AmazonClientException;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.google.protobuf.ByteString;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Stream;

@Singleton
public class SnapshotProvider {
    private static final Logger LOG = LogManager.getLogger(SnapshotProvider.class);

    private final Map<String, Snapshot> snapshots = new HashMap<>(); // snapshot id -> snapshot slot
    private final Map<String, String> name2id = new HashMap<>(); // slot name -> snapshot id

    private final StorageRepositories<Stream<ByteString>> storageRepositories = new StorageRepositories<>();

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

        Repository<Stream<ByteString>> s3Repo = getS3RepositoryForSnapshots(snapshotData.getStorageConfig());

        boolean s3ContainsSnapshot;
        try {
            s3ContainsSnapshot = s3Repo.contains(uri); // request to s3
        } catch (AmazonClientException e) {
            LOG.error("Unable to connect to S3 storage: {}", e.getMessage(), e);
            throw new CreateSlotException(e);
        }

        LzySlot lzySlot = switch (instance.spec().direction()) {
            case INPUT -> {
                if (snapshots.containsKey(snapshotId) || s3ContainsSnapshot) {
                    throw new SnapshotUniquenessException("Snapshot with id '" + snapshotId +
                        "' already associated with data");
                }

                yield getOrCreateSnapshot(s3Repo, snapshotId, uri).setInputSlot(instance, null);
            }
            case OUTPUT -> {
                if (!snapshots.containsKey(snapshotId) && !s3ContainsSnapshot) {
                    throw new SnapshotNotFound("Snapshot with id '" + snapshotId + "' not found");
                }

                yield getOrCreateSnapshot(s3Repo, snapshotId, uri).addOutputSlot(instance);
            }
        };

        name2id.put(instance.name(), snapshotId);

        return lzySlot;
    }

    private Snapshot getOrCreateSnapshot(Repository<Stream<ByteString>> s3Repo, String snapshotId,
                                         URI uri) throws CreateSlotException
    {
        try {
            return snapshots.computeIfAbsent(snapshotId,
                id -> {
                    try {
                        return new S3Snapshot(id, uri, s3Repo);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        } catch (Exception e) {
            throw new CreateSlotException(e.getMessage());
        }
    }

    private static String endpointFrom(LMST.StorageConfig storageConfig) throws CreateSlotException {
        return switch (storageConfig.getCredentialsCase()) {
            case S3 -> storageConfig.getS3().getEndpoint();
            case AZURE -> StorageConnectionString.create(storageConfig.getAzure().getConnectionString(), null)
                .getBlobEndpoint().getPrimaryUri();
            default -> throw new CreateSlotException("Unsupported type of S3 storage");
        };
    }

    private Repository<Stream<ByteString>> getS3RepositoryForSnapshots(LMST.StorageConfig storageConfig)
        throws CreateSlotException
    {
        return switch (storageConfig.getCredentialsCase()) {
            case S3 -> storageRepositories.getOrCreate(storageConfig.getS3().getEndpoint(),
                storageConfig.getS3().getAccessToken(), storageConfig.getS3().getSecretToken(),
                new ByteStringStreamConverter());
            case AZURE -> storageRepositories.getOrCreate(storageConfig.getAzure().getConnectionString(),
                new ByteStringStreamConverter());
            default -> throw new CreateSlotException("Unsupported type of S3 storage");
        };
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
