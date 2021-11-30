package ru.yandex.cloud.ml.platform.lzy.servant.snapshot;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecution;

import java.net.URI;
import java.net.URISyntaxException;

public class EmptyExecutionSnapshot implements ExecutionSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);

    @Override
    public URI getSlotUri(Slot slot) {
        // do nothing
        try {
            return new URI("https://some_address");
        } catch (URISyntaxException e) {
            // never happens
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onChunkInput(ByteString chunk, Slot slot) {
        // do nothing
        LOG.info("EmptyExecutionSnapshot::onChunkInput invoked with slot " + slot.name());
    }

    @Override
    public void onChunkOutput(ByteString chunk, Slot slot) {
        // do nothing
        LOG.info("EmptyExecutionSnapshot::onChunkOutput invoked with slot " + slot.name());
    }

    @Override
    public boolean isEmpty(Slot slot) {
        return true;
    }

    @Override
    public void onFinish(Slot slot) {
        // do nothing
        LOG.info("EmptyExecutionSnapshot::onFinish invoked with slot " + slot.name());
    }
}
