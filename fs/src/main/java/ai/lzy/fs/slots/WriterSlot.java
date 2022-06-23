package ai.lzy.fs.slots;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Writer;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesInSlot;

public class WriterSlot extends LzyInputSlotBase {
    private Writer writer;

    public WriterSlot(String tid, TextLinesInSlot definition) {
        super(tid, definition);
    }

    public void setStream(Writer wri) {
        this.writer = wri;
    }

    @Override
    protected void onChunk(ByteString bytes) throws IOException {
        writer.append(bytes.toStringUtf8());
    }
}
