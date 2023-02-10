package ai.lzy.portal.slots;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.storage.Repository;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class SnapshotInputSlot extends LzyInputSlotBase implements SnapshotSlot {
    private static final Logger LOG = LogManager.getLogger(SnapshotInputSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final Path storage;
    private final OutputStream outputStream;

    private final URI uri;
    private final Repository<Stream<ByteString>> repository;

    private final S3Snapshot slot;
    private SnapshotSlotStatus state = SnapshotSlotStatus.INITIALIZING;

    private final Runnable slotSyncHandler;

    public SnapshotInputSlot(SlotInstance slotInstance, S3Snapshot slot, Path storage, URI uri,
                             Repository<Stream<ByteString>> repository, @Nullable Runnable syncHandler)
        throws IOException
    {
        super(slotInstance);
        this.slot = slot;
        this.storage = storage;
        this.outputStream = Files.newOutputStream(storage);
        this.uri = uri;
        this.repository = repository;
        this.slotSyncHandler = syncHandler;
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        slot.getState().set(S3Snapshot.State.PREPARING);
        super.connect(slotUri, dataProvider);
        LOG.info("Attempt to connect to " + slotUri + " slot " + this);

        onState(LMS.SlotStatus.State.OPEN, () -> {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Error while closing file {}: {}", storage, e.getMessage(), e);
            }
        });

        var t = new Thread(READER_TG, () -> {
            // read all data to local storage (file), then OPEN the slot
            readAll();
            slot.getState().set(S3Snapshot.State.DONE);
            synchronized (slot) {
                slot.notifyAll();
            }
            // store local snapshot to S3
            try {
                state = SnapshotSlotStatus.SYNCING;
                FileChannel channel = FileChannel.open(storage, StandardOpenOption.READ);
                repository.put(uri, OutFileSlot.readFileChannel(definition().name(), 0, channel, () -> true, LOG));
                state = SnapshotSlotStatus.SYNCED;
                if (slotSyncHandler != null) {
                    slotSyncHandler.run();
                }
            } catch (Exception e) {
                LOG.error("Error while storing slot '{}' content in s3 storage: {}", name(), e.getMessage(), e);
                state = SnapshotSlotStatus.FAILED;
            }
        }, "reader-from-" + slotUri + "-to-" + definition().name());
        t.start();

        onState(LMS.SlotStatus.State.DESTROYED, t::interrupt);
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        super.onChunk(bytes);
        outputStream.write(bytes.toByteArray());
    }

    @Override
    public synchronized void destroy() {
        super.destroy();
        try {
            outputStream.close();
        } catch (IOException e) {
            LOG.warn("Can not close storage for {}: {}", this, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "SnapshotInputSlot: " + definition().name() + " -> " + storage.toString();
    }

    @Override
    public SnapshotSlotStatus snapshotState() {
        return state;
    }
}
