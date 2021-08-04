package ru.yandex.cloud.ml.platform.lzy.servant.slots;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.util.concurrent.ForkJoinPool;

public abstract class LzySlotBase implements LzySlot {
    protected static ForkJoinPool fjPool = ForkJoinPool.commonPool();
    private final Slot definition;
    private Operations.SlotStatus.State state = Operations.SlotStatus.State.UNBOUND;

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
        return this.state;
    }

    public synchronized void state(Operations.SlotStatus.State newState) {
        state = newState;
    }

    /* Waits for specific state or slot close **/
    protected synchronized boolean waitForState(Operations.SlotStatus.State state) {
        while (state != this.state && this.state != Operations.SlotStatus.State.CLOSED) {
            try {
                wait();
            }
            catch (InterruptedException ignore) {}
        }
        return state == this.state;
    }
}
