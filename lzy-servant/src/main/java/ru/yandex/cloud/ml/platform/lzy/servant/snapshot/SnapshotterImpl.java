package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import java.net.URI;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

public class SnapshotterImpl implements Snapshotter {
    private final SlotSnapshotProvider snapshotProvider;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final IAM.Auth auth;
    private final Logger logger = LogManager.getLogger(SnapshotterImpl.class);

    public SnapshotterImpl(IAM.Auth auth, String bucket,
                           SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi,
                           StorageClient storage, String sessionId) {
        this.snapshotApi = snapshotApi;
        this.auth = auth;
        snapshotProvider = new SlotSnapshotProvider.Cached(slot ->
            new S3SlotSnapshot(sessionId, bucket, slot, storage)
        );
    }

    @Override
    public void prepare(Slot slot, String snapshotId, String entryId) {
        final URI uri = snapshotProvider().slotSnapshot(slot).uri();
        final LzyWhiteboard.SnapshotEntry.Builder entryBuilder = LzyWhiteboard.SnapshotEntry.newBuilder()
            .setEntryId(entryId)
            .setStorageUri(uri.toString());

        LzyWhiteboard.PrepareCommand command = LzyWhiteboard.PrepareCommand
            .newBuilder()
            .setSnapshotId(snapshotId)
            .setEntry(entryBuilder.build())
            .setAuth(auth)
            .build();
        LzyWhiteboard.OperationStatus status = snapshotApi.prepareToSave(command);
        if (status.getStatus().equals(LzyWhiteboard.OperationStatus.Status.FAILED)) {
            throw new RuntimeException("LzyExecution::configureSlot failed to save to snapshot");
        }
    }

    @Override
    public void commit(Slot slot, String snapshotId, String entryId) {
        LzyWhiteboard.CommitCommand commitCommand = LzyWhiteboard.CommitCommand
            .newBuilder()
            .setSnapshotId(snapshotId)
            .setEntryId(entryId)
            .setEmpty(snapshotProvider().slotSnapshot(slot).isEmpty())
            .setAuth(auth)
            .build();
        LzyWhiteboard.OperationStatus status = snapshotApi.commit(commitCommand);
        if (status.getStatus().equals(LzyWhiteboard.OperationStatus.Status.FAILED)) {
            throw new RuntimeException("LzyExecution::configureSlot failed to commit to whiteboard");
        }
    }

    @Override
    public SlotSnapshotProvider snapshotProvider() {
        return snapshotProvider;
    }

    @Override
    public String storageUrlForEntry(String snapshotId, String entryId) {
        LzyWhiteboard.EntryStatusResponse response = snapshotApi.entryStatus(
            LzyWhiteboard.EntryStatusCommand.newBuilder()
                .setAuth(auth)
                .setEntryId(entryId)
                .setSnapshotId(snapshotId)
                .build());
        return response.getStorageUri();
    }
}
