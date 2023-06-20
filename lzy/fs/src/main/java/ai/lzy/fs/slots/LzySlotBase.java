package ai.lzy.fs.slots;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.v1.common.LMS;
import com.google.protobuf.ByteString;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static ai.lzy.v1.common.LMS.SlotStatus.State.*;
import static java.util.Collections.synchronizedMap;

public class LzySlotBase implements LzySlot {
    private static final Logger LOG = LogManager.getLogger(LzySlotBase.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SlotInstance slotInstance;
    private final List<Consumer<ByteString>> trafficTrackers = new CopyOnWriteArrayList<>();
    private final AtomicReference<LMS.SlotStatus.State> state = new AtomicReference<>(UNBOUND);

    private final Map<LMS.SlotStatus.State, List<StateChangeAction>> actions = synchronizedMap(new HashMap<>());
    private final Queue<LMS.SlotStatus.State> changeStateQueue = new LinkedList<>();

    protected LzySlotBase(SlotInstance slotInstance) {
        this.slotInstance = slotInstance;
        executor.execute(this::stateWatchingThread);
    }

    private void stateWatchingThread() {
        LMS.SlotStatus.State newState;
        do {
            synchronized (changeStateQueue) {
                while (changeStateQueue.isEmpty() && state.get() != DESTROYED) {
                    try {
                        changeStateQueue.wait();
                    } catch (InterruptedException e) {
                        LOG.warn("State watching thread was interrupted", e);
                    }
                }
                newState = changeStateQueue.poll();
                LOG.info("stateWatchingThread poll state {}", newState);
            }

            final LMS.SlotStatus.State finalNewState = newState;
            for (var action: actions.getOrDefault(newState, List.of())) {
                LOG.info("Running action for slot {} with state {}", name(), finalNewState);
                try {
                    action.run();
                } catch (Throwable e) {
                    action.onError(e);
                    break;
                }
            }
        } while (newState != DESTROYED);
    }

    @Override
    public SlotInstance instance() {
        return slotInstance;
    }

    @Override
    public String name() {
        return slotInstance.name();
    }

    @Override
    public Slot definition() {
        return slotInstance.spec();
    }

    public LMS.SlotStatus.State state() {
        return state.get();
    }

    public synchronized void state(LMS.SlotStatus.State newState) {
        if (state.get() == newState || state.get() == DESTROYED) {
            return;
        }

        LOG.info("Slot `{}` change state {} -> {}.", name(), state, newState);
        state.set(newState);

        synchronized (changeStateQueue) {
            changeStateQueue.add(newState);
            changeStateQueue.notifyAll();
        }

        notifyAll();
    }

    @Override
    public void onState(LMS.SlotStatus.State state, StateChangeAction action) {
        actions.computeIfAbsent(state, s -> new CopyOnWriteArrayList<>()).add(action);
    }

    @Override
    public void onState(LMS.SlotStatus.State state, Runnable action) {
        actions.computeIfAbsent(state, s -> new CopyOnWriteArrayList<>()).add(new StateChangeAction() {
            @Override
            public void onError(Throwable th) {
                LOG.fatal("Uncaught exception in slot {}.", name(), th);

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
    public void onState(Set<LMS.SlotStatus.State> state, StateChangeAction action) {
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
    public synchronized void destroy(@Nullable String error) {
        if (Set.of(CLOSED, DESTROYED).contains(state.get())) {
            close();
        }

        if (DESTROYED != state.get()) {
            state(DESTROYED);
        }
    }

    @Override
    public LMS.SlotStatus status() {
        return LMS.SlotStatus.newBuilder().build();
    }

    /* Waits for specific state or slot close **/
    @SuppressWarnings({"WeakerAccess", "SameParameterValue"})
    protected synchronized void waitForState(LMS.SlotStatus.State state) {
        while (!Set.of(state, DESTROYED).contains(this.state.get())) {
            try {
                wait();
            } catch (InterruptedException ignore) {
                // Ignored exception
            }
        }
    }
}
