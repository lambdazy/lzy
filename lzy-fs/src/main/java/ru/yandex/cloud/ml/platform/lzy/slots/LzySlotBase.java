package ru.yandex.cloud.ml.platform.lzy.slots;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzySlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static yandex.cloud.priv.datasphere.v2.lzy.Operations.SlotStatus.State.*;

public class LzySlotBase implements LzySlot {
    private static final Logger LOG = LogManager.getLogger(LzySlotBase.class);
    private final Slot definition;
    private final Map<Operations.SlotStatus.State, List<Runnable>> actions =
        Collections.synchronizedMap(new HashMap<>());
    private final AtomicReference<List<Consumer<ByteString>>> trafficTrackers = new AtomicReference<>(List.of());
    private Operations.SlotStatus.State state = Operations.SlotStatus.State.UNBOUND;

    protected LzySlotBase(Slot definition) {
        this.definition = definition;
        onState(Operations.SlotStatus.State.OPEN, () -> LOG.info("LzySlot::OPEN " + this.definition.name()));
        onState(DESTROYED, () -> LOG.info("LzySlot::DESTROYED " + this.definition.name()));
        onState(SUSPENDED, () -> LOG.info("LzySlot::SUSPENDED " + this.definition.name()));
    }

    @Override
    public String name() {
        return definition.name();
    }

    @Override
    public Slot definition() {
        return definition;
    }

    public Operations.SlotStatus.State state() {
        return this.state;
    }

    public synchronized void state(Operations.SlotStatus.State newState) {
        if (state == newState || state == DESTROYED) {
            return;
        }
        LOG.info("Slot " + name() + " changed state " + this.state + " -> " + newState);
        state = newState;
        notifyAll();
        ForkJoinPool.commonPool().execute(() -> actions.getOrDefault(newState, List.of()).forEach(Runnable::run));
    }

    @Override
    public void onState(Operations.SlotStatus.State state, Runnable action) {
        actions.computeIfAbsent(state, s -> new ArrayList<>()).add(action);
    }

    @Override
    public void onState(Set<Operations.SlotStatus.State> state, Runnable action) {
        state.forEach(s -> onState(s, action));
    }

    @Override
    public void onChunk(Consumer<ByteString> trafficTracker) {
        List<Consumer<ByteString>> list;
        List<Consumer<ByteString>> oldTrackers;
        do {
            oldTrackers = trafficTrackers.get();
            list = new ArrayList<>(oldTrackers);
            list.add(trafficTracker);
        } while (!trafficTrackers.compareAndSet(oldTrackers, list));
    }

    protected void onChunk(ByteString chunk) throws IOException {
        trafficTrackers.get().forEach(c -> c.accept(chunk));
    }

    @Override
    public void suspend() {
        if (!Set.of(CLOSED, DESTROYED, SUSPENDED).contains(state))
            state(SUSPENDED);
    }

    @Override
    public void close() {
        if (!Set.of(CLOSED, DESTROYED, SUSPENDED).contains(state))
            suspend();
        if (!Set.of(CLOSED, DESTROYED).contains(state))
            state(CLOSED);
    }

    @Override
    public void destroy() {
        if (Set.of(CLOSED, DESTROYED).contains(state))
            close();
        if (DESTROYED != state)
            state(DESTROYED);
    }

    @Override
    public Operations.SlotStatus status() {
        return Operations.SlotStatus.newBuilder().build();
    }

    /* Waits for specific state or slot close **/
    @SuppressWarnings({"WeakerAccess", "SameParameterValue"})
    protected synchronized void waitForState(Operations.SlotStatus.State state) {
        while (state != this.state && this.state != DESTROYED) {
            try {
                wait();
            } catch (InterruptedException ignore) {
                // Ignored exception
            }
        }
    }
}
