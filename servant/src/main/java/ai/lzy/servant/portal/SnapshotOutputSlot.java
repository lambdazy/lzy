package ai.lzy.servant.portal;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.fs.slots.OutFileSlot;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

public class SnapshotOutputSlot extends LzySlotBase implements LzyOutputSlot {
    private final Path storage;

    public SnapshotOutputSlot(SlotInstance slotInstance, Path storage) {
        super(slotInstance);
        this.storage = storage;
        assert Files.exists(storage);
    }

    @Override
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        // TODO: wait for being ready (?)
        var channel = FileChannel.open(storage, StandardOpenOption.READ);
        return OutFileSlot.readFileChannel(definition().name(), offset, channel, () -> true);
    }
}
