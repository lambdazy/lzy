package ai.lzy.fs.slots;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.Slot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.v1.Operations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.*;
import static ai.lzy.v1.Operations.SlotStatus.State.*;

public class LzySlotBase implements LzySlot {
    private static final Logger LOG = LogManager.getLogger(LzySlotBase.class);

    private final Slot definition;
    private final Map<Operations.SlotStatus.State, List<StateChangeAction>> actions = synchronizedMap(new HashMap<>());
    private final List<Consumer<ByteString>> trafficTrackers = new CopyOnWriteArrayList<>();
    private final AtomicReference<Operations.SlotStatus.State> state = new AtomicReference<>(UNBOUND);

    protected LzySlotBase(Slot definition) {
        this.definition = definition;
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
        return state.get();
    }

    public synchronized void state(Operations.SlotStatus.State newState) {
        if (state.get() == newState || state.get() == DESTROYED) {
            return;
        }

        LOG.info("Slot `{}` change state {} -> {}.", name(), state, newState);
        state.set(newState);

        actions.getOrDefault(newState, List.of()).forEach(
            action -> {
                if (state.get() == newState) {
                    try {
                        action.run();
                    } catch (Throwable e) {
                        action.onError(e);
                    }
                } else {
                    LOG.error("Slot `{}` state has changed (expected {}, got {}), don't run obsolete action ({}).",
                        definition.name(), newState, state.get(), action);
                }
            });

        notifyAll();
    }

    @Override
    public void onState(Operations.SlotStatus.State state, StateChangeAction action) {
        actions.computeIfAbsent(state, s -> new CopyOnWriteArrayList<>()).add(action);
    }

    @Override
    public void onState(Operations.SlotStatus.State state, Runnable action) {
        actions.computeIfAbsent(state, s -> new CopyOnWriteArrayList<>()).add(new StateChangeAction() {
            @Override
            public void onError(Throwable th) {
                LOG.fatal("Uncaught exception in slot {}.", definition.name(), th);

                for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                    if (element.getClassName().startsWith("org.junit.")) {
                        System.exit(-1);
                    }
                }
            }

            @Override
            public void run() {
                action.run();
            }
        });
    }

    @Override
    public void onState(Set<Operations.SlotStatus.State> state, StateChangeAction action) {
        state.forEach(s -> onState(s, action));
    }

    @Override
    public void onChunk(Consumer<ByteString> trafficTracker) {
        trafficTrackers.add(trafficTracker);
    }

    protected void onChunk(ByteString chunk) throws IOException {
        trafficTrackers.forEach(c -> c.accept(chunk));
    }

    @Override
    public synchronized void suspend() {
        if (!Set.of(CLOSED, DESTROYED, SUSPENDED).contains(state.get())) {
            state(SUSPENDED);
        }
    }

    @Override
    public synchronized void close() {
        if (!Set.of(CLOSED, DESTROYED, SUSPENDED).contains(state.get())) {
            suspend();
        }

        if (!Set.of(CLOSED, DESTROYED).contains(state.get())) {
            state(CLOSED);
        }
    }

    @Override
    public synchronized void destroy() {
        if (Set.of(CLOSED, DESTROYED).contains(state.get())) {
            close();
        }

        if (DESTROYED != state.get()) {
            state(DESTROYED);
        }
    }

    @Override
    public Operations.SlotStatus status() {
        return Operations.SlotStatus.newBuilder().build();
    }

    /* Waits for specific state or slot close **/
    @SuppressWarnings({"WeakerAccess", "SameParameterValue"})
    protected synchronized void waitForState(Operations.SlotStatus.State state) {
        while (!Set.of(state, DESTROYED).contains(this.state.get())) {
            try {
                wait();
            } catch (InterruptedException ignore) {
                // Ignored exception
            }
        }
    }
}
