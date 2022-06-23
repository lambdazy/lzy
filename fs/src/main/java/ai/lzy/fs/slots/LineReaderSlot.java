package ai.lzy.fs.slots;

import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public class LineReaderSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(LineReaderSlot.class);

    private final String tid;
    private final CompletableFuture<LineNumberReader> reader = new CompletableFuture<>();
    private long offset = 0;

    public LineReaderSlot(String tid, TextLinesOutSlot definition) {
        super(definition);
        state(Operations.SlotStatus.State.OPEN);
        this.tid = tid;
    }

    public void setStream(LineNumberReader lnr) {
        this.reader.complete(lnr);
    }

    @Override
    public Operations.SlotStatus status() {
        return Operations.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(GrpcConverter.to(definition()))
            .setTaskId(tid)
            .build();
    }


    @Override
    public void close() {
        super.close();
        reader.completeExceptionally(new RuntimeException("Force closed"));
    }

    @Override
    public synchronized Stream<ByteString> readFromPosition(long offset) throws IOException {
        if (offset != 0) {
            throw new EOFException();
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private ByteString line;

            @Override
            public boolean hasNext() {
                try {
                    String line = reader.get().readLine();
                    if (line == null)
                        return false;
                    this.line = ByteString.copyFromUtf8(line + "\n");
                    return true;
                } catch (IOException | InterruptedException | ExecutionException e) {
                    LOG.warn("Unable to read line from reader", e);
                    line = null;
                    return false;
                }
            }

            @Override
            public ByteString next() {
                if (line == null && !hasNext()) {
                    throw new NoSuchElementException();
                }
                try {
                    LOG.debug("Send from slot {} some data", name());
                    LineReaderSlot.this.offset += line.size();
                    return line;
                } finally {
                    try {
                        onChunk(line);
                    } catch (Exception re) {
                        LOG.warn("Error in traffic tracker", re);
                    }
                    line = null;
                }
            }
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.DISTINCT), false);
    }
}
