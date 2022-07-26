package ai.lzy.fs.slots;

import ai.lzy.model.SlotInstance;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final int NUM_EXECUTORS = 8;
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(NUM_EXECUTORS);

    private final SlotInstance slotInstance;
    private final List<Consumer<ByteString>> trafficTrackers = new CopyOnWriteArrayList<>();
    private final AtomicReference<Operations.SlotStatus.State> state = new AtomicReference<>(UNBOUND);

    private final Map<Operations.SlotStatus.State, List<StateChangeAction>> actions = synchronizedMap(new HashMap<>());
    private final Queue<Operations.SlotStatus.State> changeStateQueue = new LinkedList<>();

    protected LzySlotBase(SlotInstance slotInstance) {
        this.slotInstance = slotInstance;
        EXECUTOR.execute(this::stateWatchingThread);
    }

    private void stateWatchingThread() {
        Operations.SlotStatus.State newState;
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

            final Operations.SlotStatus.State finalNewState = newState;
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

    public Operations.SlotStatus.State state() {
        return state.get();
    }

    public synchronized void state(Operations.SlotStatus.State newState) {
        if (state.get() == newState || state.get() == DESTROYED) {
            return;
        }

        LOG.info("Slot `{}` change state {} -> {}.", name(), state, newState);
        state.set(newState);

        synchronized (changeStateQueue) {
            changeStateQueue.add(newState);
            changeStateQueue.notify();
        }

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
