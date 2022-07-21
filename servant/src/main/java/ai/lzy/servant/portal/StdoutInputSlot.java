package ai.lzy.servant.portal;

import ai.lzy.fs.slots.LzyInputSlotBase;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.v1.Operations;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;

public class StdoutInputSlot extends LzyInputSlotBase {
    private static final Logger LOG = LogManager.getLogger(StdoutInputSlot.class);
    private static final ThreadGroup READER_TG = new ThreadGroup("input-slot-readers");

    private final StdoutSlot stdoutSlot;

    public StdoutInputSlot(SlotInstance slotInstance, StdoutSlot stdoutSlot) {
        super(slotInstance);
        this.stdoutSlot = stdoutSlot;
    }

    @Override
    public void connect(URI slotUri, Stream<ByteString> dataProvider) {
        super.connect(slotUri, dataProvider);
        LOG.info("Attempt to connect to " + slotUri + " slot " + this);

        var t = new Thread(READER_TG, this::readAll, "reader-from-" + slotUri + "-to-" + definition().name());
        t.start();

        onState(Operations.SlotStatus.State.DESTROYED, t::interrupt);
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        super.onChunk(bytes);
        stdoutSlot.onLine(name(), bytes);
    }

    @Override
    public synchronized void destroy() {
        super.destroy();
        stdoutSlot.detach(name());
    }

    @Override
    public String toString() {
        return "StdoutInputSlot: " + definition().name();
    }
}
