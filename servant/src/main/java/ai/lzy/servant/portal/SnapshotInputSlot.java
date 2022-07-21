package ai.lzy.servant.portal;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.model.Slot;
import ai.lzy.v1.Operations;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class SnapshotInputSlot extends LzyInputSlotBase {
    private static final Logger LOG = LogManager.getLogger(SnapshotInputSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final Path storage;
    private final OutputStream outputStream;

    public SnapshotInputSlot(String taskId, Slot definition, Path storage) throws IOException {
        super(taskId, definition);
        this.storage = storage;
        this.outputStream = Files.newOutputStream(storage);
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        super.connect(slotUri, dataProvider);
        LOG.info("Attempt to connect to " + slotUri + " slot " + this);

        onState(Operations.SlotStatus.State.OPEN, () -> {
            try {
                outputStream.close();
            } catch (IOException e) {
                LOG.error("Error while closing file {}: {}", storage, e.getMessage(), e);
            }
        });

        var t = new Thread(READER_TG, this::readAll, "reader-from-" + slotUri + "-to-" + definition().name());
        t.start();

        onState(Operations.SlotStatus.State.DESTROYED, t::interrupt);
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
}
