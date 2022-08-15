package ai.lzy.servant.portal;

import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.servant.portal.Portal.CreateSlotException;
import ai.lzy.servant.portal.s3.S3Repository;
import ai.lzy.servant.portal.slots.SnapshotInputSlot;
import ai.lzy.servant.portal.slots.SnapshotOutputSlot;
import ai.lzy.servant.portal.slots.SnapshotSlot;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class S3SnapshotSlot implements SnapshotSlot {
    private static final Logger LOG = LogManager.getLogger(S3SnapshotSlot.class);

    private final String snapshotId;

    private final AtomicReference<SnapshotInputSlot> inputSlot = new AtomicReference<>(null);
    private final Map<String, SnapshotOutputSlot> outputSlots = new ConcurrentHashMap<>(); // slot name -> slot instance

    private final String key;
    private final String bucket;
    private final S3Repository<Stream<ByteString>> s3Repository;
    private final Path storageFile;

    public final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);

    public S3SnapshotSlot(String snapshotId, String key, String bucket, S3Repository<Stream<ByteString>> s3Repository)
        throws IOException {
        this.snapshotId = snapshotId;
        this.key = key;
        this.bucket = bucket;
        this.s3Repository = s3Repository;
        this.storageFile = Files.createTempFile("lzy", "snapshot_" + snapshotId + "_storage");
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
    public SnapshotOutputSlot addOutputSlot(SlotInstance slotInstance) throws CreateSlotException {
        assert slotInstance.spec().direction() == Slot.Direction.OUTPUT;
        var outputSlot = new SnapshotOutputSlot(Objects.nonNull(inputSlot.get()),
            slotInstance, this, storageFile, key, bucket, s3Repository);
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

    public enum State {
        INITIAL,
        PREPARING,
        DONE
    }
}
