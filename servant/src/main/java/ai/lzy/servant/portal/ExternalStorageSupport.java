package ai.lzy.servant.portal;

import ai.lzy.fs.snapshot.SlotSnapshot;
import ai.lzy.fs.snapshot.SlotSnapshotImpl;
import ai.lzy.fs.storage.StorageClient;
import ai.lzy.model.Slot;
import ai.lzy.v1.SnapshotApiGrpc;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

final class ExternalStorageSupport {

    interface S3ClientProvider {
        StorageClient get();
    }

    private final String portalTaskId;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final Map<S3ClientProvider, StorageClient> storageClients = new HashMap<>();
    private final Map<S3ClientProvider, SlotSnapshot> slotSnapshots = new HashMap<>();

    ExternalStorageSupport(String portalTaskId, SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi) {
        this.portalTaskId = portalTaskId;
        this.snapshotApi = snapshotApi;
    }

    synchronized SlotSnapshot createSlotSnapshot(S3ClientProvider s3key, String bucket, Slot slot) {
        return slotSnapshots.computeIfAbsent(s3key, k ->
            new SlotSnapshotImpl(portalTaskId, bucket, slot, storageClients.computeIfAbsent(k, S3ClientProvider::get)));
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
