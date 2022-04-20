package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
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
import static yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State.*;
import static yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State.OPEN;

public class SnapshooterImpl implements Snapshooter {
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final IAM.Auth auth;
    private final Set<String> trackedSlots = new HashSet<>();
    private final StorageClient storage;
    private final String sessionId;
    private final String bucket;
    private boolean closed = false;

    public SnapshooterImpl(IAM.Auth auth, String bucket,
                           SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi,
                           StorageClient storage, String sessionId) {
        this.snapshotApi = snapshotApi;
        this.auth = auth;
        this.storage = storage;
        this.bucket = bucket;
        this.sessionId = sessionId;
    }

    @Override
    public synchronized void registerSlot(LzySlot slot, String snapshotId, String entryId) {
        if (closed) {
            throw new RuntimeException("Snapshooter is already closed");
        }

        final SlotSnapshot snapshot = new S3SlotSnapshot(sessionId, bucket, slot.definition(), storage);

        final URI uri = snapshot.uri();
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
        slot.onChunk(snapshot::onChunk);

        slot.onState(OPEN, () -> {
            synchronized (SnapshooterImpl.this) {
                try {
                    commit(snapshotId, entryId, snapshot);
                } finally {
                    slot.suspend();
                }
                trackedSlots.remove(slot.name());
                SnapshooterImpl.this.notifyAll();
            }
        });

        slot.onState(DESTROYED, () -> {
            synchronized (SnapshooterImpl.this) {
                if (!trackedSlots.contains(slot.name())) {  // Already committed in OPEN
                    return;
                }
                commit(snapshotId, entryId, snapshot);
                trackedSlots.remove(slot.name());
                SnapshooterImpl.this.notifyAll();
            }
        });

        this.notifyAll();
    }

    private synchronized void commit(String snapshotId, String entryId, SlotSnapshot snapshot) {
        snapshot.onFinish();
        final LzyWhiteboard.CommitCommand commitCommand = LzyWhiteboard.CommitCommand
                .newBuilder()
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .setEmpty(snapshot.isEmpty())
                .setAuth(auth)
                .setErrored(false)
                .build();
        final LzyWhiteboard.OperationStatus status = snapshotApi.commit(commitCommand);
        if (status.getStatus().equals(FAILED)) {
            throw new RuntimeException("LzyExecution::configureSlot failed to commit to whiteboard");
        }
    }

    @Override
    public synchronized void close() throws InterruptedException {
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
