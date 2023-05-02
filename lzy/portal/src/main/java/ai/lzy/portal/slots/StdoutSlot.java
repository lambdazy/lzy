package ai.lzy.portal.slots;

import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlotBase;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.model.slot.TextLinesOutSlot;
import ai.lzy.portal.exceptions.CreateSlotException;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.slots.LSA;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.commons.collections4.queue.CircularFifoQueue;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class StdoutSlot extends LzyOutputSlotBase {
    private final Map<String, StdoutInputSlot> task2slot = new HashMap<>();
    private final Map<String, String> slot2task = new HashMap<>();

    // TODO(artolord) replace this buffer with persistent queue or file to prevent OOM
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

        log.info("attach slot " + slotInstance.spec() + ", task " + taskId);

        slot2task.put(slotInstance.name(), taskId);

        var lzySlot = new StdoutInputSlot(slotInstance, this);
        task2slot.put(taskId, lzySlot);

        notifyAll();

        return lzySlot;
    }

    public synchronized void detach(String slot) {
        log.info("detach slot " + slot);
        var taskId = slot2task.remove(slot);
        if (taskId != null) {
            task2slot.remove(taskId);
            if (slot2task.isEmpty() && finishing.get()) {
                log.info("Stdout slot <{}> is finished, completing stream", name());
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
            log.error("Attempt to write stdout/stderr slot from unknown task, slot " + slot);
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
    public void readFromPosition(long offset, StreamObserver<LSA.SlotDataChunk> responseObserver) {
        assert offset == 0;  // This is stream slot

        while (true) {

            if (((ServerCallStreamObserver<LSA.SlotDataChunk>) responseObserver).isCancelled()) {
                log.error("Stream cancelled, returning...");
                return;
            }

            var l = buffer.peek();
            while (l != null) {
                if (((ServerCallStreamObserver<LSA.SlotDataChunk>) responseObserver).isCancelled()) {
                    log.error("Stream cancelled, returning...");
                    return;
                }
                try {
                    responseObserver.onNext(LSA.SlotDataChunk.newBuilder()
                        .setChunk(ByteString.copyFromUtf8(l))
                        .build());
                    buffer.poll();
                } catch (Exception e) {
                    log.error("Error while sending data from slot {}", name(), e);
                    responseObserver.onError(Status.INTERNAL.asException());
                    return;
                }
                l = buffer.peek();
            }

            if (finished.get()) {
                break;
            }

            try {
                synchronized (StdoutSlot.this) {
                    StdoutSlot.this.wait();
                }
            } catch (InterruptedException e) {
                // ignored
            }
        }

        responseObserver.onNext(LSA.SlotDataChunk.newBuilder()
            .setControl(LSA.SlotDataChunk.Control.EOS)
            .build());
        responseObserver.onCompleted();

        completedReads.getAndIncrement();
    }

    public synchronized void finish() {
        if (finishing.compareAndSet(false, true)) {
            if (slot2task.isEmpty()) {
                log.info("Stdout slot <{}> is finished, completing stream", name());
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
