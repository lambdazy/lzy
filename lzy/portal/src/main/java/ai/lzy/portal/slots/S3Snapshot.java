package ai.lzy.portal.slots;

import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.portal.storage.Repository;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class S3Snapshot implements Snapshot {
    private final String snapshotId;

    private final AtomicReference<SnapshotInputSlot> inputSlot = new AtomicReference<>(null);
    private final Map<String, SnapshotOutputSlot> outputSlots = new ConcurrentHashMap<>(); // slot name -> slot instance

    private final URI uri;
    private final Repository<Stream<ByteString>> repository;
    private final Path storageFile;

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);

    public S3Snapshot(String snapshotId, URI uri, Repository<Stream<ByteString>> repository)
        throws IOException
    {
        this.snapshotId = snapshotId;
        this.uri = uri;
        this.repository = repository;
        this.storageFile = Files.createTempFile("lzy_", "_storage");
        this.storageFile.toFile().deleteOnExit();
    }

    public AtomicReference<State> getState() {
        return state;
    }

    @Override
    public SnapshotInputSlot setInputSlot(SlotInstance instance, @Nullable Runnable syncHandler)
        throws CreateSlotException
    {
        assert instance.spec().direction() == Slot.Direction.INPUT;
        try {
            var inputSlot = new SnapshotInputSlot(instance, this, storageFile, uri, repository, syncHandler);
            if (!this.inputSlot.compareAndSet(null, inputSlot)) {
                throw new CreateSlotException("InputSlot already set for snapshot " + snapshotId);
            }
            return inputSlot;
        } catch (IOException e) {
            throw new CreateSlotException(e.getMessage());
        }
    }

    @Override
    public SnapshotOutputSlot addOutputSlot(SlotInstance slotInstance) {
        assert slotInstance.spec().direction() == Slot.Direction.OUTPUT;
        var outputSlot = new SnapshotOutputSlot(slotInstance, this, storageFile, uri, repository);
        var prev = outputSlots.putIfAbsent(slotInstance.name(), outputSlot);
        assert prev == null;
        return outputSlot;
    }

    @Override
    @Nullable
    public SnapshotInputSlot getInputSlot() {
        return inputSlot.get();
    }

    @Override
    @Nullable
    public SnapshotOutputSlot getOutputSlot(String slotName) {
        return outputSlots.get(slotName);
    }

    @Override
    public Collection<SnapshotOutputSlot> getOutputSlots() {
        return outputSlots.values();
    }

    @Override
    public boolean removeInputSlot(String slotName) {
        var inputSlot = this.inputSlot.get();
        if (Objects.nonNull(inputSlot) && inputSlot.name().equals(slotName)) {
            return this.inputSlot.compareAndSet(inputSlot, null);
        }
        return false;
    }

    @Override
    public boolean removeOutputSlot(String slotName) {
        return Objects.nonNull(outputSlots.remove(slotName));
    }

    public enum State {
        INITIAL,
        PREPARING,
        DONE
    }
}
