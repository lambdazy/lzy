package ai.lzy.portal.slots;

import ai.lzy.model.slot.Slot;
import ai.lzy.portal.exceptions.CreateSlotException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

final class SnapshotEntry {
    public enum State {
        INITIAL,
        PREPARING,
        DONE
    }

    private final URI storageUri;
    private final Path tempfile;
    private final AtomicReference<SnapshotInputSlot> input = new AtomicReference<>(null);
    private final Map<String, SnapshotOutputSlot> outputs = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);

    public SnapshotEntry(URI storageUri) throws IOException {
        this.storageUri = storageUri;
        this.tempfile = Files.createTempFile("lzy_", "_snapshot");
        this.tempfile.toFile().deleteOnExit();
    }

    public URI getStorageUri() {
        return storageUri;
    }

    public Path getTempfile() {
        return tempfile;
    }

    public AtomicReference<State> getState() {
        return state;
    }

    @Nullable
    public SnapshotInputSlot getInputSlot() {
        return input.get();
    }

    @Nullable
    public SnapshotOutputSlot getOutputSlot(String slotName) {
        return outputs.get(slotName);
    }

    public Collection<SnapshotOutputSlot> getOutputSlots() {
        return outputs.values();
    }

    public void setInputSlot(SnapshotInputSlot slot) throws CreateSlotException {
        assert slot.definition().direction() == Slot.Direction.INPUT;
        if (!this.input.compareAndSet(null, slot)) {
            throw new CreateSlotException("Snapshot entry already associated with input slot");
        }
    }

    public void addOutputSlot(SnapshotOutputSlot slot) throws CreateSlotException {
        assert slot.definition().direction() == Slot.Direction.OUTPUT;
        if (outputs.putIfAbsent(slot.name(), slot) != null) {
            throw new CreateSlotException("Snapshot entry already has output slot with same name");
        }
    }

    public boolean removeInputSlot(String slotName) {
        var inputSlot = this.input.get();
        if (inputSlot != null && inputSlot.name().equals(slotName)) {
            return this.input.compareAndSet(inputSlot, null);
        }
        return false;
    }

    public boolean removeOutputSlot(String slotName) {
        return outputs.remove(slotName) != null;
    }
}
