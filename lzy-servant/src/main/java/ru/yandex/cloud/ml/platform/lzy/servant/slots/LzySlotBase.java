package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SlotSnapshotProvider;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public class LzySlotBase implements LzySlot {
    private static final Logger LOG = LogManager.getLogger(LzySlotBase.class);
    protected final Snapshotter snapshotter;
    private final Slot definition;
    private final Map<Operations.SlotStatus.State, List<Runnable>> actions =
        Collections.synchronizedMap(new HashMap<>());
    private Operations.SlotStatus.State state = Operations.SlotStatus.State.UNBOUND;

    protected String entryId;
    protected String snapshotId;
    private final AtomicBoolean snapshotSet = new AtomicBoolean(false);

    protected LzySlotBase(Slot definition, Snapshotter snapshotter) {
        this.snapshotter = snapshotter;
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
        if (state == newState) {
            return;
        }
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
            } catch (InterruptedException ignore) {
                // Ignored exception
            }
        }
    }

    @Override
    public void snapshot(String snapshotId, String entryId) {
        this.snapshotId = snapshotId;
        this.entryId = entryId;
        snapshotSet.set(true);
        if (this.definition().direction() == Slot.Direction.OUTPUT) {
            snapshotter.prepare(definition, snapshotId, entryId);
        }
    }

    @Override
    public boolean throughSnapshot() {
        return snapshotSet.get();
    }
}
