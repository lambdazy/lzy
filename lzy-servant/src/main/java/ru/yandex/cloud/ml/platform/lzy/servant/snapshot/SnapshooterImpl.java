package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard.OperationStatus.Status.FAILED;
import static yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State.DESTROYED;

public class SnapshooterImpl implements Snapshooter {
    private final SlotSnapshotProvider snapshotProvider;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final IAM.Auth auth;
    private final Set<String> trackedSlots = new HashSet<>();
    private final StorageClient storage;
    private boolean closed = false;

    public SnapshooterImpl(IAM.Auth auth, String bucket,
                           SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi,
                           StorageClient storage, String sessionId) {
        this.snapshotApi = snapshotApi;
        this.auth = auth;
        this.storage = storage;
        snapshotProvider = new SlotSnapshotProvider.Cached(slot ->
            new S3SlotSnapshot(sessionId, bucket, slot, storage)
        );
    }

    @Override
    public synchronized void registerSlot(LzySlot slot, String snapshotId, String entryId) {
        if (closed) {
            throw new RuntimeException("Snapshooter is already closed");
        }
        final URI uri = snapshotProvider.slotSnapshot(slot.definition()).uri();
        if (slot instanceof LzyInputSlot) {
            throw new RuntimeException("Input slot snapshooting is not supported yet");
        }

        final LzyWhiteboard.SnapshotEntry.Builder entryBuilder = LzyWhiteboard.SnapshotEntry.newBuilder()
            .setEntryId(entryId)
            .setStorageUri(uri.toString());

        final LzyWhiteboard.PrepareCommand command = LzyWhiteboard.PrepareCommand
            .newBuilder()
            .setSnapshotId(snapshotId)
            .setEntry(entryBuilder.build())
            .setAuth(auth)
            .build();
        if (snapshotApi.prepareToSave(command).getStatus().equals(FAILED)) {
            throw new RuntimeException("LzyExecution::configureSlot failed to save to snapshot");
        }

        trackedSlots.add(slot.name());
        slot.onChunk(chunk -> {
            snapshotProvider.slotSnapshot(slot.definition()).onChunk(chunk);
        });

        slot.onState(Set.of(DESTROYED), () -> {
            final LzyWhiteboard.CommitCommand commitCommand = LzyWhiteboard.CommitCommand
                .newBuilder()
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .setEmpty(snapshotProvider.slotSnapshot(slot.definition()).isEmpty())
                .setAuth(auth)
                .build();
            final LzyWhiteboard.OperationStatus status = snapshotApi.commit(commitCommand);
            synchronized (SnapshooterImpl.this) {
                trackedSlots.remove(slot.name());
                SnapshooterImpl.this.notifyAll();
            }
            if (status.getStatus().equals(FAILED)) {
                throw new RuntimeException("LzyExecution::configureSlot failed to commit to whiteboard");
            }
        });
        this.notifyAll();
    }

    @Override
    public synchronized void close() throws Exception {
        closed = true;
        while (!trackedSlots.isEmpty()) {
            this.wait();
        }
    }

    @Override
    public StorageClient storage() {
        return storage;
    }
}
