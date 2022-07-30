package ai.lzy.servant.portal;

import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class SnapshotSlot {
    private static final Logger LOG = LogManager.getLogger(SnapshotSlot.class);

    private final String snapshotId;
    private final Path storageFile;
    private final AtomicReference<SnapshotInputSlot> inputSlot = new AtomicReference<>(null);
    private final Map<String, SnapshotOutputSlot> outputSlots = new ConcurrentHashMap<>(); // slot name -> slot instance

    SnapshotSlot(String snapshotId) throws IOException {
        this.snapshotId = snapshotId;
        this.storageFile = Files.createTempFile("lzy", "snapshot_" + snapshotId + "_storage");
    }

    SnapshotInputSlot setInputSlot(SlotInstance slotInstance) throws IOException {
        assert slotInstance.spec().direction() == Slot.Direction.INPUT;
        var inputSlot = new SnapshotInputSlot(slotInstance, storageFile);
        if (!this.inputSlot.compareAndSet(null, inputSlot)) {
            LOG.error("InputSlot already set for snapshot " + snapshotId);
            throw new RuntimeException("InputSlot already set for snapshot " + snapshotId);
        }
        return inputSlot;
    }

    String getSnapshotId() {
        return snapshotId;
    }

    @Nullable
    SnapshotInputSlot getInputSlot() {
        return inputSlot.get();
    }

    @Nullable
    SnapshotOutputSlot getOutputSlot(String slotName) {
        return outputSlots.get(slotName);
    }

    Iterable<SnapshotOutputSlot> getOutputSlots() {
        return outputSlots.values();
    }

    SnapshotOutputSlot addOutputSlot(SlotInstance slotInstance) {
        assert slotInstance.spec().direction() == Slot.Direction.OUTPUT;
        var outputSlot = new SnapshotOutputSlot(slotInstance, storageFile);
        var prev = outputSlots.putIfAbsent(slotInstance.name(), outputSlot);
        assert prev == null;
        return outputSlot;
    }

    boolean removeOutputSlot(String slotName) {
        return outputSlots.remove(slotName) != null;
    }

    boolean removeInputSlot(String slotName) {
        var inputSlot = this.inputSlot.get();
        if (inputSlot != null && inputSlot.name().equals(slotName)) {
            return this.inputSlot.compareAndSet(inputSlot, null);
        }
        return false;
    }

    void close() {
        inputSlot.set(null);
        outputSlots.clear();
        try {
            Files.deleteIfExists(storageFile);
        } catch (IOException e) {
            LOG.warn("Can not remove snapshot '{}' storage file '{}': {}", snapshotId, storageFile, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
