package ai.lzy.fs.snapshot;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.GrpcConverter;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyWhiteboard;
import ai.lzy.v1.SnapshotApiGrpc;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static ai.lzy.v1.LzyWhiteboard.OperationStatus.Status.FAILED;
import static ai.lzy.v1.Operations.SlotStatus.State.*;
import static ai.lzy.v1.Operations.SlotStatus.State.OPEN;

public class SnapshooterImpl implements Snapshooter {
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi;
    private final IAM.Auth auth;
    private final Set<String> trackedSlots = new HashSet<>();
    private final SlotSnapshotProvider snapshotProvider;
    private boolean closed = false;

    public SnapshooterImpl(IAM.Auth auth, SnapshotApiGrpc.SnapshotApiBlockingStub snapshotApi,
                           SlotSnapshotProvider snapshotProvider) {
        this.snapshotApi = snapshotApi;
        this.auth = auth;
        this.snapshotProvider = snapshotProvider;
    }

    @Override
    public synchronized void registerSlot(LzySlot slot, String snapshotId, String entryId) {
        if (closed) {
            throw new RuntimeException("Snapshooter is already closed");
        }

        final SlotSnapshot slotSnapshot = snapshotProvider.slotSnapshot(slot.instance());

        final URI uri = slotSnapshot.uri();
        if (slot instanceof LzyInputSlot) {
            throw new RuntimeException("Input slot snapshooting is not supported yet");
        }

        final LzyWhiteboard.SnapshotEntry.Builder entryBuilder = LzyWhiteboard.SnapshotEntry.newBuilder()
            .setEntryId(entryId)
            .setStorageUri(uri.toString())
            .setType(GrpcConverter.to(slot.definition().contentType()));

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
        slot.onChunk(slotSnapshot::onChunk);

        slot.onState(OPEN, () -> {
            synchronized (SnapshooterImpl.this) {
                try {
                    commit(snapshotId, entryId, slotSnapshot);
                } finally {
                    slot.suspend();
                    trackedSlots.remove(slot.name());
                    SnapshooterImpl.this.notifyAll();
                }
            }
        });

        slot.onState(DESTROYED, () -> {
            synchronized (SnapshooterImpl.this) {
                if (!trackedSlots.contains(slot.name())) {  // Already committed in OPEN
                    return;
                }
                abort(snapshotId, entryId);
                trackedSlots.remove(slot.name());
                SnapshooterImpl.this.notifyAll();
            }
        });

        this.notifyAll();
    }

    private synchronized void commit(String snapshotId, String entryId, SlotSnapshot snapshot) {
        try {
            snapshot.onFinish();
            final LzyWhiteboard.CommitCommand commitCommand = LzyWhiteboard.CommitCommand
                .newBuilder()
                .setSnapshotId(snapshotId)
                .setEntryId(entryId)
                .setEmpty(snapshot.isEmpty())
                .setAuth(auth)
                .build();
            final LzyWhiteboard.OperationStatus status = snapshotApi.commit(commitCommand);
            if (status.getStatus().equals(FAILED)) {
                throw new RuntimeException("Failed to commit to whiteboard");
            }
        } catch (Exception e) {
            abort(snapshotId, entryId);
            throw new RuntimeException(e);
        }

    }

    private void abort(String snapshotId, String entryId) {
        final LzyWhiteboard.AbortCommand abortCommand = LzyWhiteboard.AbortCommand
            .newBuilder()
            .setSnapshotId(snapshotId)
            .setEntryId(entryId)
            .setAuth(auth)
            .build();
        final LzyWhiteboard.OperationStatus status = snapshotApi.abort(abortCommand);
    }

    @Override
    public synchronized void close() throws InterruptedException {
        closed = true;
        while (!trackedSlots.isEmpty()) {
            this.wait();
        }
    }
}
