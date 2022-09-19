package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.s3.ByteStringStreamConverter;
import ai.lzy.portal.s3.S3Repositories;
import ai.lzy.portal.s3.S3Repository;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortal.PortalSlotDesc.Snapshot;
import com.amazonaws.AmazonClientException;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.lzy.portal.Portal.CreateSlotException;

public class SnapshotSlotsProvider {
    private static final Logger LOG = LogManager.getLogger(SnapshotSlotsProvider.class);

    private final Map<String, SnapshotSlot> snapshots = new HashMap<>(); // snapshot id -> snapshot slot
    private final Map<String, String> name2id = new HashMap<>(); // slot name -> snapshot id

    private final S3Repositories<Stream<ByteString>> s3Repositories = new S3Repositories<>();

    public synchronized LzySlot createSlot(Snapshot snapshotData, SlotInstance instance) throws CreateSlotException {
        String key = snapshotData.getS3().getKey();
        String bucket = snapshotData.getS3().getBucket();
        String endpoint = endpointFrom(snapshotData.getS3());

        var snapshotId = "%s-%s-%s".formatted(key, bucket, endpoint).replaceAll("/", "");
        var previousSnapshotId = name2id.get(instance.name());
        if (Objects.nonNull(previousSnapshotId)) {
            throw new CreateSlotException("Slot '" + instance.name() + "' already associated with "
                + "snapshot '" + previousSnapshotId + "'");
        }

        S3Repository<Stream<ByteString>> s3Repo = getS3RepositoryForSnapshots(snapshotData.getS3());

        boolean s3ContainsSnapshot;
        try {
            s3ContainsSnapshot = s3Repo.contains(bucket, key); // request to s3
        } catch (AmazonClientException e) {
            LOG.error("Unable to connect to S3 storage: {}", e.getMessage(), e);
            throw new CreateSlotException(e);
        }

        LzySlot lzySlot = switch (instance.spec().direction()) {
            case INPUT -> {
                if (snapshots.containsKey(snapshotId) || s3ContainsSnapshot) {
                    throw new CreateSlotException("Snapshot with id '" + snapshotId + "' already associated with data");
                }

                yield getOrCreateSnapshotSlot(s3Repo, snapshotId, key, bucket).setInputSlot(instance);
            }
            case OUTPUT -> {
                if (!snapshots.containsKey(snapshotId) && !s3ContainsSnapshot) {
                    throw new CreateSlotException("Snapshot with id '" + snapshotId + "' not found");
                }

                yield getOrCreateSnapshotSlot(s3Repo, snapshotId, key, bucket).addOutputSlot(instance);
            }
        };

        name2id.put(instance.name(), snapshotId);

        return lzySlot;
    }

    private SnapshotSlot getOrCreateSnapshotSlot(S3Repository<Stream<ByteString>> s3Repo, String snapshotId,
                                                 String key, String bucket) throws CreateSlotException {
        try {
            return snapshots.computeIfAbsent(snapshotId,
                id -> {
                    try {
                        return new S3SnapshotSlot(id, key, bucket, s3Repo);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        } catch (Exception e) {
            throw new CreateSlotException(e.getMessage());
        }
    }

    private static String endpointFrom(LMS3.S3Locator s3Locator) throws CreateSlotException {
        return switch (s3Locator.getEndpointCase()) {
            case AMAZON -> s3Locator.getAmazon().getEndpoint();
            case AZURE -> StorageConnectionString.create(s3Locator.getAzure().getConnectionString(), null)
                .getBlobEndpoint().getPrimaryUri();
            default -> throw new CreateSlotException("Unsupported type of S3 storage");
        };
    }

    private S3Repository<Stream<ByteString>> getS3RepositoryForSnapshots(LMS3.S3Locator s3Locator)
        throws CreateSlotException {
        return switch (s3Locator.getEndpointCase()) {
            case AMAZON -> s3Repositories.getOrCreate(s3Locator.getAmazon().getEndpoint(),
                s3Locator.getAmazon().getAccessToken(), s3Locator.getAmazon().getSecretToken(),
                new ByteStringStreamConverter());
            case AZURE -> s3Repositories.getOrCreate(s3Locator.getAzure().getConnectionString(),
                new ByteStringStreamConverter());
            default -> throw new CreateSlotException("Unsupported type of S3 storage");
        };
    }

    public boolean removeInputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) && ss.removeInputSlot(slotName);
    }

    public boolean removeOutputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) && ss.removeOutputSlot(slotName);
    }

    public Collection<? extends LzyInputSlot> getInputSlots() {
        return snapshots.values().stream().map(SnapshotSlot::getInputSlot).filter(Objects::nonNull).toList();
    }

    public Collection<? extends LzyOutputSlot> getOutputSlots() {
        return snapshots.values().stream().flatMap(slot -> slot.getOutputSlots().stream()).toList();
    }

    public LzyInputSlot getInputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) ? ss.getInputSlot() : null;
    }

    public LzyOutputSlot getOutputSlot(String slotName) {
        SnapshotSlot ss = snapshots.get(name2id.get(slotName));
        return Objects.nonNull(ss) ? ss.getOutputSlot(slotName) : null;
    }
}
