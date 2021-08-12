package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.slots.TextLinesOutSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.EOFException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LineReaderSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(LineReaderSlot.class);

    private final String tid;
    private LineNumberReader reader;
    private long offset = 0;
    private List<Runnable> closeActions = new ArrayList<>();

    public LineReaderSlot(String tid, TextLinesOutSlot definition) {
        super(definition);
        this.tid = tid;
    }

    public void setStream(LineNumberReader lnr) {
        this.reader = lnr;
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
    public Stream<ByteString> readFromPosition(long offset) throws IOException {
        if (offset != 0) {
            throw new EOFException();
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private String line;
            @Override
            public boolean hasNext() {
                try {
                    line = reader.readLine();
                    return line != null;
                }
                catch (IOException e) {
                    LOG.warn("Unable to read line from reader", e);
                    line = null;
                    return false;
                }
                finally {
                    if (line == null) {
                        close();
                    }
                }
            }

            @Override
            public ByteString next() {
                if (line == null && !hasNext())
                    throw new NoSuchElementException();
                //noinspection ConstantConditions
                final ByteString bytes = ByteString.copyFromUtf8(line);
                LineReaderSlot.this.offset += bytes.size();
                line = null;
                return bytes;
            }
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.DISTINCT), false);
    }

    public void close() {
        ForkJoinPool.commonPool().execute(() -> {
            Thread.yield();
            closeActions.forEach(Runnable::run);
        });
    }


    @Override
    public void onClose(Runnable action) {
        closeActions.add(action);
    }
}
