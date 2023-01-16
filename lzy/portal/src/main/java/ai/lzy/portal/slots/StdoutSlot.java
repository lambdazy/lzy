package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.fs.slots.LzySlotBase;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StdoutSlot extends LzySlotBase implements LzyOutputSlot {
    private static final Logger LOG = LogManager.getLogger(StdoutSlot.class);

    private final Map<String, StdoutInputSlot> task2slot = new HashMap<>();
    private final Map<String, String> slot2task = new HashMap<>();
    private final CircularFifoQueue<String> buffer = new CircularFifoQueue<>(1024);
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean finishing = new AtomicBoolean(false);

    public StdoutSlot(String name, String portalTaskId, String channelId, URI slotUri) {
        super(new SlotInstance(new TextLinesOutSlot(name), portalTaskId, channelId, slotUri));
        state(LMS.SlotStatus.State.OPEN);

        onState(LMS.SlotStatus.State.DESTROYED, () -> {
            finished.set(true);
            synchronized (StdoutSlot.this) {
                StdoutSlot.this.notifyAll();
            }
        });
    }

    @Nonnull
    public synchronized LzySlot attach(SlotInstance slotInstance) throws CreateSlotException {
        if (finished.get() || finishing.get()) {
            throw new IllegalStateException("Cannot attach because of finished portal");
        }
        final String taskId = slotInstance.taskId();
        if (task2slot.containsKey(taskId)) {
            throw new CreateSlotException("Slot " + slotInstance.name() + " from task "
                + taskId + " already exists");
        }

        LOG.info("attach slot " + slotInstance.spec() + ", task " + taskId);

        slot2task.put(slotInstance.name(), taskId);

        var lzySlot = new StdoutInputSlot(slotInstance, this);
        task2slot.put(taskId, lzySlot);

        notifyAll();

        return lzySlot;
    }

    public synchronized void detach(String slot) {
        LOG.info("detach slot " + slot);
        var taskId = slot2task.remove(slot);
        if (taskId != null) {
            task2slot.remove(taskId);
            if (slot2task.isEmpty() && finishing.get()) {
                LOG.info("Stdout slot <{}> is finished, completing stream", name());
                finished.set(true);
            }
            notifyAll();
        }
    }

    public synchronized void onLine(String slot, ByteString line) {
        var taskId = slot2task.get(slot);
        if (taskId != null) {
            if (!line.isEmpty()) {
                buffer.offer("[LZY-REMOTE-" + taskId + "] - " + line.toStringUtf8());
            }
            notifyAll();
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
        }, Spliterator.IMMUTABLE | Spliterator.ORDERED), /* parallel */ false);
    }

    public synchronized void finish() {
        if (finishing.compareAndSet(false, true)) {
            if (slot2task.isEmpty()) {
                LOG.info("Stdout slot <{}> is finished, completing stream", name());
                finished.set(true);
                notifyAll();
            }

            for (var slot : task2slot.values()) {
                if (slot.state().equals(LMS.SlotStatus.State.UNBOUND)) {
                    slot.close();
                }
            }
        }
    }

    public synchronized void await() throws InterruptedException {
        while (!finished.get()) {
            wait();
        }
    }
}
