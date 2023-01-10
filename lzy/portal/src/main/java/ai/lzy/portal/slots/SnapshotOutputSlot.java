package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.storage.Repository;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.lzy.v1.common.LMS.SlotStatus.State.OPEN;

public class SnapshotOutputSlot extends LzySlotBase implements LzyOutputSlot, SnapshotSlot {
    private static final Logger LOG = LogManager.getLogger(SnapshotOutputSlot.class);

    private final URI uri;
    private final Repository<Stream<ByteString>> repository;
    private final Path storage;

    private final boolean hasInputSlot;

    private final S3Snapshot slot;
    private SnapshotSlotStatus state = SnapshotSlotStatus.INITIALIZING;

    public SnapshotOutputSlot(SlotInstance slotInstance, S3Snapshot slot, Path storage,
                              URI uri, Repository<Stream<ByteString>> repository)
    {
        super(slotInstance);
        this.uri = uri;
        this.repository = repository;
        this.storage = storage;
        this.slot = slot;
        this.hasInputSlot = Objects.nonNull(slot.getInputSlot());
    }

    private static void write(Stream<ByteString> data, File sink) throws IOException {
        try (var storage = new BufferedOutputStream(new FileOutputStream(sink))) {
            data.forEach(chunk -> {
                try {
                    LOG.debug("Received chunk of size {}", chunk.size());
                    chunk.writeTo(storage);
                } catch (IOException ioe) {
                    LOG.warn("Unable write chunk of data of size " + chunk.size()
                        + " to file " + sink.getAbsolutePath(), ioe);
                }
            });
        }
    }

    @Override
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        if (hasInputSlot) {
            if (slot.getState().get() == S3Snapshot.State.INITIAL) {
                LOG.error("Input slot of this snapshot is not already connected");
                state = SnapshotSlotStatus.FAILED;
                throw new IllegalStateException("Input slot of this snapshot is not already connected");
            }
        } else if (slot.getState().compareAndSet(S3Snapshot.State.INITIAL, S3Snapshot.State.PREPARING)) {
            state = SnapshotSlotStatus.SYNCING;
            try {
                write(repository.get(uri), storage.toFile());
            } catch (Exception e) {
                LOG.error("Cannot sync data with remote storage", e);
                state = SnapshotSlotStatus.FAILED;
                throw e;
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
            LOG.error("Can not open file channel on file {}", storage);
            throw new RuntimeException(e);
        }
        state = SnapshotSlotStatus.SYNCED;

        FileChannel channel = FileChannel.open(storage);
        state(OPEN);
        return OutFileSlot.readFileChannel(definition().name(), offset, channel, () -> true);
    }

    @Override
    public SnapshotSlotStatus snapshotState() {
        return state;
    }
}
