package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

public class LzySlotBase implements LzySlot {
    private static final Logger LOG = LogManager.getLogger(LzySlotBase.class);
    private final Slot definition;
    private Operations.SlotStatus.State state = Operations.SlotStatus.State.UNBOUND;
    private final Map<Operations.SlotStatus.State, List<Runnable>> actions = Collections.synchronizedMap(new HashMap<>());
    protected final SlotSnapshotProvider snapshotProvider;

    protected LzySlotBase(Slot definition, SlotSnapshotProvider snapshotProvider) {
        this.snapshotProvider = snapshotProvider;
        this.definition = definition;
        onState(Operations.SlotStatus.State.OPEN, () -> LOG.info("LzySlot::OPEN " + this.definition.name()));
        onState(Operations.SlotStatus.State.DESTROYED, () -> LOG.info("LzySlot::DESTROYED " + this.definition.name()));
        onState(Operations.SlotStatus.State.SUSPENDED, () -> LOG.info("LzySlot::SUSPENDED " + this.definition.name()));
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
        if (state == newState)
            return;
        state = newState;
        notifyAll();
        ForkJoinPool.commonPool().execute(() -> actions.getOrDefault(newState, List.of()).forEach(Runnable::run));
    }

    @Override
    public void onState(Operations.SlotStatus.State state, Runnable action) {
        actions.computeIfAbsent(state, s -> new ArrayList<>()).add(action);
    }

    @Override
    public void destroy() {
        state(Operations.SlotStatus.State.DESTROYED);
    }

    @Override
    public void suspend() {
        state(Operations.SlotStatus.State.SUSPENDED);
    }

    @Override
    public Operations.SlotStatus status() {
        return Operations.SlotStatus.newBuilder().build();
    }


    /* Waits for specific state or slot close **/
    @SuppressWarnings({"WeakerAccess", "SameParameterValue"})
    protected synchronized void waitForState(Operations.SlotStatus.State state) {
        while (state != this.state && this.state != Operations.SlotStatus.State.DESTROYED) {
            try {
                wait();
            }
            catch (InterruptedException ignore) {}
        }
    }
}
