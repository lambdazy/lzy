package ai.lzy.fs;

import ai.lzy.fs.backends.FileInputBackend;
import ai.lzy.fs.backends.OutputPipeBackend;
import ai.lzy.fs.transfers.TransferFactory;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.channel.v2.LzyChannelManagerGrpc.LzyChannelManagerBlockingStub;
import ai.lzy.v1.common.LMS;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class SlotsExecutionContext {
    private static final Logger LOG = LogManager.getLogger(SlotsExecutionContext.class);
    private static final StorageClientFactory storageClientFactory = new StorageClientFactory(4, 4);

    private final List<LMS.Slot> slotDescriptions;
    private final Map<String, String> slotToChannelMapping;
    private final SlotsContext context;
    private final Path fsRoot;

    public SlotsExecutionContext(Path fsRoot, List<LMS.Slot> slotDescriptions, Map<String, String> slotToChannelMapping,
                                 LzyChannelManagerBlockingStub channelManager, String executionId,
                                 String slotsApiAddress, Supplier<String> tokenSupplier, SlotsService slotsService)
    {
        this.fsRoot = fsRoot;
        this.slotDescriptions = slotDescriptions;
        this.slotToChannelMapping = slotToChannelMapping;

        var transferFactory = new TransferFactory(storageClientFactory, tokenSupplier);

        context = new SlotsContext(channelManager, transferFactory, slotsApiAddress, slotsService, executionId, this);
    }

    private final List<ExecutionCompanion> companions = new ArrayList<>();

    public void beforeExecution() throws Exception {
        try {
            for (LMS.Slot desc : slotDescriptions) {
                String channelId = slotToChannelMapping.get(desc.getName());  // Name here is slot path on lzy fs.

                var fsPath = fsRoot.resolve(desc.getName());

                if (desc.getDirection().equals(LMS.Slot.Direction.INPUT)) {

                    var backend = new FileInputBackend(fsPath);
                    var inputSlot = new InputSlot(backend, desc.getName(), channelId, context);

                    companions.add(inputSlot);
                } else {
                    var backend = new OutputPipeBackend(fsPath);
                    var outputSlot = new OutputSlot(backend, desc.getName(), channelId, context);

                    companions.add(outputSlot);
                }
            }

            for (var slot : companions) {
                slot.beforeExecution();
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize slots", e);
            close();
            throw e;
        }
    }

    public void afterExecution() throws Exception {
        try {
            for (var slot : companions) {
                slot.afterExecution();
            }
        } catch (Exception e) {
            LOG.error("Failed to finalize slots", e);
            close();
            throw e;
        }
    }

    public synchronized void close() {
        for (var slot: companions) {
            slot.close();
        }
    }

    synchronized void add(ExecutionCompanion companion) {
        companions.add(companion);
    }

    @VisibleForTesting
    public SlotsContext context() {
        return context;
    }
}
