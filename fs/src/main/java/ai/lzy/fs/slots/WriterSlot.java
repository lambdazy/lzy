package ai.lzy.fs.slots;

import ai.lzy.model.SlotInstance;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Writer;
import ai.lzy.model.slots.TextLinesInSlot;

public class WriterSlot extends LzyInputSlotBase {
    private Writer writer;

    public WriterSlot(SlotInstance instance) {
        super(instance);
    }

    public void setStream(Writer wri) {
        this.writer = wri;
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        writer.append(bytes.toStringUtf8());
    }
}
