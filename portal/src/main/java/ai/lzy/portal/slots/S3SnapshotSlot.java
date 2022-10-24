package ai.lzy.portal.slots;

import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.portal.Portal.CreateSlotException;
import ai.lzy.portal.s3.S3Repository;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class S3SnapshotSlot implements SnapshotSlot {
    private final String snapshotId;

    private final AtomicReference<SnapshotInputSlot> inputSlot = new AtomicReference<>(null);
    private final Map<String, SnapshotOutputSlot> outputSlots = new ConcurrentHashMap<>(); // slot name -> slot instance

    private final String key;
    private final String bucket;
    private final S3Repository<Stream<ByteString>> s3Repository;
    private final Path storageFile;

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);

    public S3SnapshotSlot(String snapshotId, String key, String bucket, S3Repository<Stream<ByteString>> s3Repository)
        throws IOException
    {
        this.snapshotId = snapshotId;
        this.key = key;
        this.bucket = bucket;
        this.s3Repository = s3Repository;
        this.storageFile = Files.createTempFile("lzy", "snapshot_" + snapshotId + "_storage");
        this.storageFile.toFile().deleteOnExit();
    }

    public AtomicReference<State> getState() {
        return state;
    }

    @Override
    public SnapshotInputSlot setInputSlot(SlotInstance slotInstance) throws CreateSlotException {
        assert slotInstance.spec().direction() == Slot.Direction.INPUT;
        try {
            var inputSlot = new SnapshotInputSlot(slotInstance, this, storageFile, key, bucket, s3Repository);
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
        var outputSlot = new SnapshotOutputSlot(slotInstance, this, storageFile, key, bucket, s3Repository);
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
