package ai.lzy.slots;

import ai.lzy.slots.backends.FileInputBackend;
import ai.lzy.slots.backends.OutputPipeBackend;
import ai.lzy.slots.transfers.TransferFactory;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.channel.LzyChannelManagerGrpc.LzyChannelManagerBlockingStub;
import ai.lzy.v1.common.LMS;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
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
    private final List<SlotInternal> slots = new ArrayList<>();

    public SlotsExecutionContext(Path fsRoot, List<LMS.Slot> slotDescriptions, Map<String, String> slotToChannelMapping,
                                 LzyChannelManagerBlockingStub channelManager, String requestId, String executionId,
                                 String taskId, String slotsApiAddress, Supplier<String> tokenSupplier,
                                 SlotsService slotsService)
    {
        this.fsRoot = fsRoot;
        this.slotDescriptions = slotDescriptions;
        this.slotToChannelMapping = slotToChannelMapping;

        var transferFactory = new TransferFactory(storageClientFactory, tokenSupplier);

        context = new SlotsContext(channelManager, transferFactory, slotsApiAddress, slotsService,
            requestId, executionId, taskId, this);
    }

    public void beforeExecution() throws Exception {
        try {
            for (LMS.Slot desc : slotDescriptions) {
                String channelId = slotToChannelMapping.get(desc.getName());  // Name here is slot path on lzy fs.

                var fsPath = Path.of(fsRoot.toString(), desc.getName());

                if (desc.getDirection() == LMS.Slot.Direction.INPUT) {
                    var backend = new FileInputBackend(fsPath);
                    var inputSlot = new InputSlot(backend, desc.getName(), channelId, context);

                    slots.add(inputSlot);
                } else {
                    var backend = new OutputPipeBackend(fsPath);
                    var outputSlot = new OutputSlot(backend, desc.getName(), channelId, context);

                    slots.add(outputSlot);
                }
            }

            final CompletableFuture<?>[] futures;

            synchronized (this) {
                futures = slots.stream()
                    .map(SlotInternal::beforeExecution)
                    .toArray(CompletableFuture[]::new);
            }

            CompletableFuture.allOf(futures).join();
        } catch (Exception e) {
            LOG.error("Failed to initialize slots", e);
            close();
            throw e;
        }
    }

    public void afterExecution() {
        try {
            final CompletableFuture<?>[] futures;

            synchronized (this) {
                futures = slots.stream()
                    .map(SlotInternal::afterExecution)
                    .toArray(CompletableFuture[]::new);
            }

            CompletableFuture.allOf(futures).join();
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

    synchronized void add(SlotInternal companion) {
        slots.add(companion);
    }

    @VisibleForTesting
    public SlotsContext context() {
        return context;
    }
}
