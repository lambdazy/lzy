package ai.lzy.servant.portal;

import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.s3.S3SnapshotInputSlot;
import ai.lzy.servant.portal.s3.S3SnapshotOutputSlot;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class ExternalStorage {

    public interface S3ClientProvider {
        StorageClient get();
    }

    private final Map<S3ClientProvider, StorageClient> storageClients = new HashMap<>();
    private final Map<S3ClientProvider, S3SnapshotOutputSlot> toLoad = new HashMap<>();
    private final Map<S3ClientProvider, S3SnapshotInputSlot> toStore = new HashMap<>();

    private final Map<String, S3ClientProvider> slotToS3Key = new HashMap<>(); // slotName -> s3Key

    S3SnapshotOutputSlot getOutputSlot(String slotName) {
        return toLoad.get(slotToS3Key.get(slotName));
    }

    S3SnapshotInputSlot getInputSlot(String slotName) {
        return toStore.get(slotToS3Key.get(slotName));
    }

    Collection<S3SnapshotInputSlot> getInputSlots() {
        return toStore.values();
    }

    Collection<S3SnapshotOutputSlot> getOutputSlots() {
        return toLoad.values();
    }

    void removeInputSlot(String slotName) {
        S3ClientProvider s3Key = slotToS3Key.get(slotName);
        toStore.remove(s3Key);
        if (!toLoad.containsKey(s3Key)) {
            slotToS3Key.remove(slotName);
        }
    }

    void removeOutputSlot(String slotName) {
        S3ClientProvider s3Key = slotToS3Key.get(slotName);
        toLoad.remove(s3Key);
        if (!toStore.containsKey(s3Key)) {
            slotToS3Key.remove(slotName);
        }
    }

    S3SnapshotInputSlot createSlotSnapshot(SlotInstance instance, String key, String bucket, S3ClientProvider s3key) {
        return toStore.computeIfAbsent(s3key, k -> {
            slotToS3Key.put(instance.name(), s3key);
            return new S3SnapshotInputSlot(instance, key, bucket,
                    storageClients.computeIfAbsent(k, S3ClientProvider::get));
        });
    }

    S3SnapshotOutputSlot readSlotSnapshot(SlotInstance instance, String key, String bucket, S3ClientProvider s3key) {
        return toLoad.computeIfAbsent(s3key, k -> {
            slotToS3Key.put(instance.name(), s3key);
            return new S3SnapshotOutputSlot(instance, key, bucket,
                    storageClients.computeIfAbsent(k, S3ClientProvider::get));
        });
    }

    record AmazonS3Key(String endpoint, String accessToken, String secretToken) implements S3ClientProvider {
        static AmazonS3Key of(String endpoint, String accessToken, String secretToken) {
            return new AmazonS3Key(endpoint, accessToken, secretToken);
        }

        @Override
        public StorageClient get() {
            return StorageClient.createAmazonS3Client(URI.create(endpoint), accessToken, secretToken);
        }
    }

    record AzureS3Key(String connectionString) implements S3ClientProvider {
        static AzureS3Key of(String connectionString) {
            return new AzureS3Key(connectionString);
        }

        @Override
        public StorageClient get() {
            return StorageClient.createAzureS3Client(connectionString);
        }
    }
}
