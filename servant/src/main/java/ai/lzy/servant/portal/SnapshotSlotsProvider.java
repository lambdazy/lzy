package ai.lzy.servant.portal;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.s3.AmazonS3RepositoryAdapter;
import ai.lzy.servant.portal.s3.ByteStringStreamConverter;
import ai.lzy.servant.portal.s3.S3Repository;
import ai.lzy.servant.portal.slots.SnapshotSlot;
import ai.lzy.v1.LzyPortalApi;
import ai.lzy.v1.LzyPortalApi.PortalSlotDesc.Snapshot;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.google.protobuf.ByteString;
import ru.yandex.qe.s3.amazon.transfer.AmazonTransmitterFactory;
import ru.yandex.qe.s3.transfer.Transmitter;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.lzy.servant.portal.Portal.CreateSlotException;

public class SnapshotSlotsProvider {
    private final Map<String, SnapshotSlot> snapshots = new HashMap<>(); // snapshot id -> snapshot slot
    private final Map<String, String> name2id = new HashMap<>(); // slot name -> snapshot id

    public synchronized LzySlot createSlot(Snapshot snapshotData, SlotInstance instance) throws CreateSlotException {
        LzyPortalApi.S3Locator s3Data = snapshotData.getS3();
        String endpoint = switch (s3Data.getEndpointCase()) {
            case AMAZON -> s3Data.getAmazon().getEndpoint();
            case AZURE -> {
                var storageConnectionString = StorageConnectionString.create(
                    s3Data.getAzure().getConnectionString(), null);
                yield storageConnectionString.getBlobEndpoint().getPrimaryUri();
            }
            default -> throw new CreateSlotException("Unsupported type of S3 storage");
        };

        var snapshotId = "%s-%s-%s".formatted(s3Data.getKey(), s3Data.getBucket(), endpoint)
            .replaceAll("/", "");
        var oldSnapshotId = name2id.get(instance.name());
        if (Objects.nonNull(oldSnapshotId)) {
            throw new CreateSlotException("Slot '" + instance.name() + "' already associated with "
                + "snapshot '" + oldSnapshotId + "'");
        }

        S3Repository<Stream<ByteString>> s3Repo = switch (s3Data.getEndpointCase()) {
            case AMAZON -> {
                BasicAWSCredentials credentials = new BasicAWSCredentials(s3Data.getAmazon().getAccessToken(),
                    s3Data.getAmazon().getSecretToken());
                AmazonS3 client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(
                        new AmazonS3ClientBuilder.EndpointConfiguration(
                            endpoint, "us-west-1"
                        )
                    )
                    .withPathStyleAccessEnabled(true)
                    .build();
                Transmitter transmitter = new AmazonTransmitterFactory(client)
                    .fixedPoolsTransmitter("transmitter", 10, 10);
                yield new AmazonS3RepositoryAdapter<>(client, transmitter, 10, new ByteStringStreamConverter());
            }
            // case AZURE -> {
            //  var client = new BlobServiceClientBuilder()
            //      .connectionString(s3Data.getAzure().getConnectionString()).buildClient();
            //  var transmitter = new AzureTransmitterFactory(client).fixedPoolsTransmitter
            //  (transmitterName, downloadsPoolSize,
            //            chunksPoolSize)
            // ...
            default -> throw new CreateSlotException("Unsupported type of S3 storage");
        };
        LzySlot lzySlot = switch (instance.spec().direction()) {
            case INPUT -> {
                if (snapshots.containsKey(snapshotId)
                    || s3Repo.containsKey(s3Data.getBucket(), s3Data.getKey())) { // request to s3
                    throw new CreateSlotException("Snapshot with id '" + snapshotId + "' already associated with data");
                }
                SnapshotSlot newSnapshotSlot = snapshots.computeIfAbsent(snapshotId,
                    id -> {
                        try {
                            return new S3SnapshotSlot(id, s3Data.getKey(), s3Data.getBucket(), s3Repo);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                yield newSnapshotSlot.setInputSlot(instance);
            }
            case OUTPUT -> {
                if (!snapshots.containsKey(snapshotId)
                    && !s3Repo.containsKey(s3Data.getBucket(), s3Data.getKey())) { // request to s3
                    throw new CreateSlotException("Snapshot with id '" + snapshotId + "' not found");
                }
                SnapshotSlot newSnapshotSlot = snapshots.computeIfAbsent(snapshotId,
                    id -> {
                        try {
                            return new S3SnapshotSlot(id, s3Data.getKey(), s3Data.getBucket(), s3Repo);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                yield newSnapshotSlot.addOutputSlot(instance);
            }
        };

        name2id.put(instance.name(), snapshotId);

        return lzySlot;
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
