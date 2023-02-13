package ai.lzy.portal.slots;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.storage.StorageClient;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class SnapshotInputSlot extends LzyInputSlotBase implements SnapshotSlot {
    private static final Logger LOG = LogManager.getLogger(SnapshotInputSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final SnapshotEntry snapshot;
    private final StorageClient storageClient;
    private final OutputStream outputStream;

    private final Runnable slotSyncHandler;
    private SnapshotSlotStatus state = SnapshotSlotStatus.INITIALIZING;

    public SnapshotInputSlot(SlotInstance slotData, SnapshotEntry snapshot, StorageClient storageClient,
                             @Nullable Runnable syncHandler)
        throws IOException
    {
        super(slotData);
        this.snapshot = snapshot;
        this.storageClient = storageClient;
        this.outputStream = Files.newOutputStream(snapshot.getTempfile());
        this.slotSyncHandler = syncHandler;
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        snapshot.getState().set(SnapshotEntry.State.PREPARING);
        super.connect(slotUri, dataProvider);
        LOG.info("Attempt to connect to " + slotUri + " slot " + this);

        onState(LMS.SlotStatus.State.OPEN, () -> {
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Error while closing file {}: {}", snapshot.getTempfile(), e.getMessage(), e);
            }
        });

        var t = new Thread(READER_TG, () -> {
            // read all data to local storage (file), then OPEN the slot
            readAll();
            snapshot.getState().set(SnapshotEntry.State.DONE);
            synchronized (snapshot) {
                snapshot.notifyAll();
            }
            // store local snapshot to S3
            try {
                state = SnapshotSlotStatus.SYNCING;
                storageClient.write(snapshot.getStorageUri(), snapshot.getTempfile());
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
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.warn("Can not close storage for {}: {}", this, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "SnapshotInputSlot: " + definition().name() + " -> " + snapshot.getTempfile().toString();
    }

    @Override
    public SnapshotSlotStatus snapshotState() {
        return state;
    }
}
