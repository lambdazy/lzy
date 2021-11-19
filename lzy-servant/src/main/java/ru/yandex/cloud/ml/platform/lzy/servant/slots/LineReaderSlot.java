package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.ExecutionSnapshot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.EOFException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LineReaderSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(LineReaderSlot.class);

    private final String tid;
    private final CompletableFuture<LineNumberReader> reader = new CompletableFuture<>();
    private long offset = 0;

    public LineReaderSlot(String tid, TextLinesOutSlot definition, ExecutionSnapshot snapshot) {
        super(definition, snapshot);
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
            .setDeclaration(gRPCConverter.to(definition()))
            .setTaskId(tid)
            .build();
    }

    @Override
    public synchronized Stream<ByteString> readFromPosition(long offset) throws IOException {
        if (offset != 0) {
            throw new EOFException();
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private String line;
            @Override
            public boolean hasNext() {
                try {
                    line = reader.get().readLine();
                    if (line == null) {
                        snapshot.onFinish(definition());
                    }
                    return line != null;
                }
                catch (IOException | InterruptedException | ExecutionException e) {
                    LOG.warn("Unable to read line from reader", e);
                    line = null;
                    snapshot.onFinish(definition());
                    return false;
                }
            }

            @Override
            public ByteString next() {
                if (line == null && !hasNext())
                    throw new NoSuchElementException();
                final ByteString bytes = ByteString.copyFromUtf8(line + "\n");
                LineReaderSlot.this.offset += bytes.size();
                LOG.info("Send from slot {} data {}", name(), line);
                line = null;
                snapshot.onChunkOutput(bytes, definition());
                return bytes;
            }
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.DISTINCT), false);
    }
}
