package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Writer;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;

public class WriterSlot extends LzyInputSlotBase {
    private Writer writer;

    public WriterSlot(String tid, TextLinesInSlot definition, Snapshotter snapshotter) {
        super(tid, definition, snapshotter);
    }

    public void setStream(Writer wri) {
        this.writer = wri;
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        writer.append(bytes.toStringUtf8());
    }
}
