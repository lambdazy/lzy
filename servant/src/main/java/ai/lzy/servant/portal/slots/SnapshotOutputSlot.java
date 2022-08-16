package ai.lzy.servant.portal.slots;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.S3SnapshotSlot;
import ai.lzy.servant.portal.s3.S3Repository;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;

import static ai.lzy.v1.Operations.SlotStatus.State.OPEN;

public class SnapshotOutputSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(SnapshotInputSlot.class);

    private final String key;
    private final String bucket;
    private final S3Repository<Stream<ByteString>> s3Repository;
    private final Path storage;

    private final boolean hasInputSlot;

    private final S3SnapshotSlot slot;

    public SnapshotOutputSlot(SlotInstance slotInstance, S3SnapshotSlot slot, Path storage,
                              String key, String bucket, S3Repository<Stream<ByteString>> s3Repository) {
        super(slotInstance);
        this.key = key;
        this.bucket = bucket;
        this.s3Repository = s3Repository;
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
            if (slot.getState().get() == S3SnapshotSlot.State.INITIAL) {
                LOG.error("Input slot of this snapshot is not already connected");
                throw new IllegalStateException("Input slot of this snapshot is not already connected");
            }
        } else if (slot.getState().compareAndSet(S3SnapshotSlot.State.INITIAL, S3SnapshotSlot.State.PREPARING)) {
            write(s3Repository.get(bucket, key), storage.toFile());
            slot.getState().set(S3SnapshotSlot.State.DONE);
            synchronized (slot) {
                slot.notifyAll();
            }
        }

        try {
            synchronized (slot) {
                while (slot.getState().get() != S3SnapshotSlot.State.DONE) {
                    slot.wait();
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Can not open file channel on file {}", storage);
            throw new RuntimeException(e);
        }

        FileChannel channel = FileChannel.open(storage);
        state(OPEN);
        return OutFileSlot.readFileChannel(definition().name(), offset, channel, () -> true);
    }
}
