package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.exceptions.SnapshotNotFound;
import ai.lzy.portal.exceptions.SnapshotUniquenessException;
import ai.lzy.portal.grpc.ProtoConverter;
import ai.lzy.portal.s3.ByteStringStreamConverter;
import ai.lzy.portal.s3.S3Repositories;
import ai.lzy.portal.s3.S3Repository;
import ai.lzy.v1.common.LMS3;
import ai.lzy.v1.portal.LzyPortal;
import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.whiteboard.LzyWhiteboardPrivateServiceGrpc;
import com.amazonaws.AmazonClientException;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.lzy.model.grpc.ProtoConverter.toProto;

public class SnapshotProvider {
    private static final Logger LOG = LogManager.getLogger(SnapshotProvider.class);

    private final Map<String, Snapshot> snapshots = new HashMap<>(); // snapshot id -> snapshot slot
    private final Map<String, String> name2id = new HashMap<>(); // slot name -> snapshot id

    private final S3Repositories<Stream<ByteString>> s3Repositories = new S3Repositories<>();
    private final LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub whiteboardClient;

    public SnapshotProvider(LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub whiteboardClient) {
        this.whiteboardClient = whiteboardClient;
    }

    public synchronized LzySlot createSlot(LzyPortal.PortalSlotDesc.Snapshot snapshotData, SlotInstance instance)
        throws CreateSlotException
    {
        String key = snapshotData.getS3().getKey();
        String bucket = snapshotData.getS3().getBucket();
        String endpoint = endpointFrom(snapshotData.getS3());

        var snapshotId = "%s-%s-%s".formatted(key, bucket, endpoint).replaceAll("/", "");
        var previousSnapshotId = name2id.get(instance.name());
        if (Objects.nonNull(previousSnapshotId)) {
            throw new SnapshotUniquenessException("Slot '" + instance.name() + "' already associated with "
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
                    throw new SnapshotUniquenessException("Snapshot with id '" + snapshotId +
                        "' already associated with data");
                }

                Runnable slotSyncHandler = null;
                if (snapshotData.hasWhiteboardRef()) {
                    var whiteboardRef = snapshotData.getWhiteboardRef();
                    var whiteboardId = whiteboardRef.getWhiteboardId();
                    var fieldName = whiteboardRef.getFieldName();

                    slotSyncHandler = () -> {
                        try {
                            var storageUri = ProtoConverter.getSlotUri(snapshotData.getS3());

                            //noinspection ResultOfMethodCallIgnored
                            whiteboardClient.linkField(LWBPS.LinkFieldRequest.newBuilder()
                                .setWhiteboardId(whiteboardId)
                                .setFieldName(fieldName)
                                .setStorageUri(storageUri)
                                .setScheme(toProto(instance.spec().contentType()))
                                .build());
                        } catch (StatusRuntimeException e) {
                            LOG.error("Cannot link whiteboard field: { whiteboardId: {}, fieldName: {}, " +
                                    "storageUri: {} }, error: {}", whiteboardId, fieldName, key,
                                e.getStatus().getDescription());
                        }
                    };
                }

                yield getOrCreateSnapshot(s3Repo, snapshotId, key, bucket).setInputSlot(instance, slotSyncHandler);
            }
            case OUTPUT -> {
                if (!snapshots.containsKey(snapshotId) && !s3ContainsSnapshot) {
                    throw new SnapshotNotFound("Snapshot with id '" + snapshotId + "' not found");
                }

                yield getOrCreateSnapshot(s3Repo, snapshotId, key, bucket).addOutputSlot(instance);
            }
        };

        name2id.put(instance.name(), snapshotId);

        return lzySlot;
    }

    private Snapshot getOrCreateSnapshot(S3Repository<Stream<ByteString>> s3Repo, String snapshotId,
                                         String key, String bucket) throws CreateSlotException
    {
        try {
            return snapshots.computeIfAbsent(snapshotId,
                id -> {
                    try {
                        return new S3Snapshot(id, key, bucket, s3Repo);
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
        throws CreateSlotException
    {
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
