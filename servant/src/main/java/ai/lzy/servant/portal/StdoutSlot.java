package ai.lzy.servant.portal;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.slots.TextLinesOutSlot;
import ai.lzy.v1.Operations;
import com.google.protobuf.ByteString;
import java.net.URI;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StdoutSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(StdoutSlot.class);

    private final Map<String, StdoutInputSlot> task2slot = new HashMap<>();
    private final Map<String, String> slot2task = new HashMap<>();
    private final CircularFifoQueue<String> buffer = new CircularFifoQueue<>(1024);
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public StdoutSlot(String name, String portalTaskId, String channelId, URI slotUri) {
        super(new SlotInstance(new TextLinesOutSlot(name), portalTaskId, channelId, slotUri));
        state(Operations.SlotStatus.State.OPEN);

        onState(Operations.SlotStatus.State.DESTROYED, () -> {
            finished.set(true);
            synchronized (StdoutSlot.this) {
                StdoutSlot.this.notify();
            }
        });
    }

    @Nullable
    public synchronized LzySlot attach(SlotInstance slotInstance) {
        final String taskId = slotInstance.taskId();
        if (task2slot.containsKey(taskId)) {
            return null;
        }

        LOG.info("attach slot " + slotInstance.spec() + ", task " + taskId);

        slot2task.put(slotInstance.name(), taskId);

        var lzySlot = new StdoutInputSlot(slotInstance, this);
        task2slot.put(taskId, lzySlot);

        notify();

        return lzySlot;
    }

    public synchronized void detach(String slot) {
        LOG.info("detach slot " + slot);
        var taskId = slot2task.remove(slot);
        if (taskId != null) {
            task2slot.remove(taskId);
            notify();
        }
    }

    public synchronized void onLine(String slot, ByteString line) {
        LOG.info("append line, slot={}, line={}", slot, line.toStringUtf8());
        var taskId = slot2task.get(slot);
        if (taskId != null) {
            buffer.offer(taskId + "; " + line.toStringUtf8());
            notify();
        } else {
            LOG.error("Attempt to write stdout/stderr slot from unknown task, slot " + slot);
        }
    }

    public synchronized void forEachSlot(Consumer<LzyInputSlot> fn) {
        task2slot.values().forEach(fn);
    }

    @Nullable
    public synchronized LzyInputSlot find(String slot) {
        var taskId = slot2task.get(slot);
        if (taskId != null) {
            return task2slot.get(taskId);
        }
        return null;
    }

    @Override
    public synchronized Stream<ByteString> readFromPosition(long offset) {
        assert offset == 0;

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private ByteString line = null;

            @Override
            public boolean hasNext() {
                synchronized (StdoutSlot.this) {
                    while (true) {
                        var l = buffer.poll();
                        if (l != null) {
                            line = ByteString.copyFromUtf8(l);
                            return true;
                        }

                        task2slot.values().forEach(slot -> LOG.debug(" ::: slot {} -> {}", slot.name(), slot.state()));

                        if (finished.get()) {
                            return false;
                        }

                        try {
                            StdoutSlot.this.wait();
                        } catch (InterruptedException e) {
                            // ignored
                        }
                    }
                }
            }

            @Override
            public ByteString next() {
                if (line == null) {
                    throw new IllegalStateException();
                }
                try {
                    LOG.debug("Send from slot {} some data: {}", name(), line.toStringUtf8());
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
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.DISTINCT), /* parallel */ false);
    }
}
