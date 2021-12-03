package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecution;

import java.net.URI;
import java.net.URISyntaxException;

public class DevNullSlotSnapshot implements SlotSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);
    private final Slot slot;

    public DevNullSlotSnapshot(Slot slot) {
        this.slot = slot;
    }

    @Override
    public URI uri() {
        // do nothing
        try {
            return new URI("https://some_address");
        } catch (URISyntaxException e) {
            // never happens
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onChunk(ByteString chunk) {
        // do nothing
        LOG.info("EmptyExecutionSnapshot::onChunk invoked with slot " + slot.name());
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public void onFinish() {
        // do nothing
        LOG.info("EmptyExecutionSnapshot::onFinish invoked with slot " + slot.name());
    }
}
