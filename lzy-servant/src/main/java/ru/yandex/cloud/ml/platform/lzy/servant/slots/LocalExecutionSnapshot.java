package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.LzyExecution;

public class LocalExecutionSnapshot implements ExecutionSnapshot {
    private static final Logger LOG = LogManager.getLogger(LzyExecution.class);
    @Override
    public void onChunkInput(ByteString chunk, Slot slot) {
        // save input chunk to Storage
        LOG.info("LocalExecutionSnapshot::onChunkInput invoked with slot " + slot.name());
    }

    @Override
    public void onChunkOutput(ByteString chunk, Slot slot) {
        // save output chunk to Storage
        LOG.info("LocalExecutionSnapshot::onChunkOutput invoked with slot " + slot.name());
    }
}
