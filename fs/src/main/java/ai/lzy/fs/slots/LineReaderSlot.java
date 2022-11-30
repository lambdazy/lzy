package ai.lzy.fs.slots;

import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.slots.TextLinesOutSlot;
import ai.lzy.v1.Operations;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LineReaderSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(LineReaderSlot.class);

    private final String tid;
    private long offset = 0;

    // Optional here to set end of queue
    private final BlockingQueue<Optional<String>> queue = new LinkedBlockingQueue<>();

    private Thread thread = null;

    public LineReaderSlot(String tid, TextLinesOutSlot definition) {
        super(definition);
        state(Operations.SlotStatus.State.OPEN);
        this.tid = tid;
    }

    public synchronized void setStream(LineNumberReader lnr) {
        if (thread != null) {
            throw new IllegalStateException("Reader already set");
        }
        thread = new Thread(() -> {
            while (true) {
                try {
                    var line = lnr.readLine();
                    LOG.info("[{} slot]: {}", name(), line);
                    queue.offer(Optional.ofNullable(line));
                    if (line == null) {
                        return;
                    }
                } catch (IOException e) {
                    LOG.error("Error while reading data for slot <{}>: ", name(), e);
                    queue.offer(Optional.empty());
                    return;
                }
            }
        });
        thread.start();
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
    public synchronized void close() {
        super.close();
        if (thread != null) {
            thread.interrupt();
        }
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
                    var line = queue.take();
                    if (line.isEmpty())
                        return false;
                    this.line = ByteString.copyFromUtf8(line.get() + "\n");
                    return true;
                } catch (InterruptedException e) {
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
