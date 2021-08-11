package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class WriterSlot extends LzyInputSlotBase {
    private Writer writer;

    public WriterSlot(String tid, TextLinesOutSlot definition) {
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
