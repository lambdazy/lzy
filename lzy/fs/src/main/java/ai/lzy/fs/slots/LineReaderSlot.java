package ai.lzy.fs.slots;

import ai.lzy.fs.fs.LzyOutputSlotBase;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class LineReaderSlot extends LzyOutputSlotBase {
    private static final Logger LOG = LogManager.getLogger(LineReaderSlot.class);

    private final CompletableFuture<LineNumberReader> reader = new CompletableFuture<>();
    private long offset = 0;

    public LineReaderSlot(SlotInstance instance) {
        super(instance);
        state(LMS.SlotStatus.State.OPEN);
    }

    public void setStream(LineNumberReader lnr) {
        this.reader.complete(lnr);
    }

    @Override
    public LMS.SlotStatus status() {
        return LMS.SlotStatus.newBuilder()
            .setState(state())
            .setPointer(offset)
            .setDeclaration(ProtoConverter.toProto(definition()))
            .setTaskId(taskId())
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
                    if (line == null) {
                        LineReaderSlot.this.destroy(null);
                        return false;
                    }
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
