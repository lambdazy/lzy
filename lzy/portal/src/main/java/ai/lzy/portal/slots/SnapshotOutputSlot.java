package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyOutputSlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.storage.StorageClient;
import ai.lzy.storage.StorageClient;
import ai.lzy.v1.slots.LSA;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static ai.lzy.v1.common.LMS.SlotStatus.State.OPEN;

public class SnapshotOutputSlot extends LzyOutputSlotBase implements SnapshotSlot {
    private final SnapshotEntry snapshot;
    private final StorageClient storageClient;

    private final boolean hasInputSlot;
    private SnapshotSlotStatus state = SnapshotSlotStatus.INITIALIZING;

    public SnapshotOutputSlot(SlotInstance slotData, SnapshotEntry snapshot, StorageClient storageClient) {
        super(slotData);
        this.snapshot = snapshot;
        this.storageClient = storageClient;
        this.hasInputSlot = snapshot.getInputSlot() != null;
    }

    @Override
    public void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver) {
        if (hasInputSlot) {
            if (snapshot.getState().get() == SnapshotEntry.State.INITIAL) {
                log.error("Input slot of this snapshot is not already connected");
                state = SnapshotSlotStatus.FAILED;
                responseObserver.onError(
                    Status.INTERNAL
                        .withDescription("Input slot of this snapshot is not already connected")
                        .asException()
                );
                return;
            }
        } else if (snapshot.getState().compareAndSet(SnapshotEntry.State.INITIAL, SnapshotEntry.State.PREPARING)) {
            state = SnapshotSlotStatus.SYNCING;
            try {
                storageClient.read(snapshot.getStorageUri(), snapshot.getTempfile());
            } catch (Exception e) {
                log.error("Cannot sync data with remote storage", e);
                state = SnapshotSlotStatus.FAILED;
                responseObserver.onError(
                    Status.INTERNAL
                        .withDescription("Cannot sync data with remote storage")
                        .asException()
                );
                return;
            }
            snapshot.getState().set(SnapshotEntry.State.DONE);
            synchronized (snapshot) {
                snapshot.notifyAll();
            }
        }

        try {
            synchronized (snapshot) {
                while (snapshot.getState().get() != SnapshotEntry.State.DONE) {
                    snapshot.wait();
                }
            }
        } catch (InterruptedException e) {
            log.error("Can not open file channel on file {}", snapshot.getTempfile());
            throw new RuntimeException(e);
        }
        state = SnapshotSlotStatus.SYNCED;

        final FileChannel channel;
        try {
            channel = FileChannel.open(snapshot.getTempfile());
        } catch (IOException e) {
            log.error("Error while creating file channel", e);
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }
        state(OPEN);

        OutFileSlot.readFileChannel(definition().name(), offset, channel, completedReads::getAndIncrement,
            responseObserver, log);
    }

    @Override
    public SnapshotSlotStatus snapshotState() {
        return state;
    }
}
