package ai.lzy.fs;

import ai.lzy.fs.backands.FileInputBackend;
import ai.lzy.fs.backands.OutputPipeBackand;
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
                                 String slotsApiAddress, Supplier<String> jwt, SlotsService slotsService)
    {
        this.fsRoot = fsRoot;
        this.slotDescriptions = slotDescriptions;
        this.slotToChannelMapping = slotToChannelMapping;

        var transferFactory = new TransferFactory(storageClientFactory, jwt);

        context = new SlotsContext(channelManager, transferFactory, slotsApiAddress, slotsService, executionId, this);
    }

    private final List<Slot> slots = Collections.synchronizedList(new ArrayList<>());

    public void beforeExecution() throws Exception {
        try {
            for (LMS.Slot desc : slotDescriptions) {
                String channelId = slotToChannelMapping.get(desc.getName());  // Name here is slot path on lzy fs.

                var fsPath = fsRoot.resolve(desc.getName());

                if (desc.getDirection().equals(LMS.Slot.Direction.INPUT)) {

                    var backend = new FileInputBackend(fsPath);
                    var inputSlot = new InputSlot(backend, desc.getName(), channelId, context);

                    slots.add(inputSlot);
                } else {
                    var backend = new OutputPipeBackand(fsPath);
                    var outputSlot = new OutputSlot(backend, desc.getName(), channelId, context);

                    slots.add(outputSlot);
                }
            }

            var futures = new ArrayList<CompletableFuture<Void>>();

            for (var slot : slots) {
                futures.add(slot.beforeExecution());
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            LOG.error("Failed to initialize slots", e);
            close();
            throw e;
        }
    }

    public void afterExecution() throws Exception {
        try {
            var futures = new ArrayList<CompletableFuture<Void>>();

            for (var slot : slots) {
                futures.add(slot.afterExecution());
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            LOG.error("Failed to finalize slots", e);
            close();
            throw e;
        }
    }

    public synchronized void close() {
        for (var slot: slots) {
            slot.close();
        }
    }

    public synchronized void addSlot(Slot slot) {
        slots.add(slot);
    }

    @VisibleForTesting
    public SlotsContext context() {
        return context;
    }
}
