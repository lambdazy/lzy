package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyOutputSlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.storage.StorageClient;
import ai.lzy.v1.slots.LSA;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

import static ai.lzy.v1.common.LMS.SlotStatus.State.OPEN;

public class SnapshotOutputSlot extends LzyOutputSlotBase implements SnapshotSlot {
    private final URI uri;
    private final StorageClient storageClient;
    private final Path storage;

    private final boolean hasInputSlot;

    private final S3Snapshot slot;
    private SnapshotSlotStatus state = SnapshotSlotStatus.INITIALIZING;

    public SnapshotOutputSlot(SlotInstance slotInstance, S3Snapshot slot, Path storage,
                              URI uri, StorageClient storageClient)
    {
        super(slotInstance);
        this.uri = uri;
        this.storage = storage;
        this.slot = slot;
        this.hasInputSlot = Objects.nonNull(slot.getInputSlot());
        this.storageClient = storageClient;
    }

    private static void write(StorageClient client, URI uri, File sink)
        throws IOException, InterruptedException
    {
        client.read(uri, sink.toPath());
    }

    @Override
    public void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver) {
        if (hasInputSlot) {
            if (slot.getState().get() == S3Snapshot.State.INITIAL) {
                log.error("Input slot of this snapshot is not already connected");
                state = SnapshotSlotStatus.FAILED;
                responseObserver.onError(
                    Status.INTERNAL
                        .withDescription("Input slot of this snapshot is not already connected")
                        .asException()
                );
                return;
            }
        } else if (slot.getState().compareAndSet(S3Snapshot.State.INITIAL, S3Snapshot.State.PREPARING)) {
            state = SnapshotSlotStatus.SYNCING;
            try {
                write(storageClient, uri, storage.toFile());
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
            slot.getState().set(S3Snapshot.State.DONE);
            synchronized (slot) {
                slot.notifyAll();
            }
        }

        try {
            synchronized (slot) {
                while (slot.getState().get() != S3Snapshot.State.DONE) {
                    slot.wait();
                }
            }
        } catch (InterruptedException e) {
            log.error("Can not open file channel on file {}", storage);
            throw new RuntimeException(e);
        }
        state = SnapshotSlotStatus.SYNCED;

        final FileChannel channel;
        try {
            channel = FileChannel.open(storage);
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
