package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Writer;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider;

public class WriterSlot extends LzyInputSlotBase {
    private Writer writer;

    public WriterSlot(String tid, TextLinesInSlot definition, SlotSnapshotProvider snapshotProvider) {
        super(tid, definition, snapshotProvider);
    }

    public void setStream(Writer wri) {
        this.writer = wri;
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        writer.append(bytes.toStringUtf8());
    }
}
