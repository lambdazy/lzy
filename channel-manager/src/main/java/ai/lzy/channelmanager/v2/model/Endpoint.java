package ai.lzy.channelmanager.v2.model;

import ai.lzy.channelmanager.v2.slot.SlotApiConnection;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;

import java.net.URI;
import javax.annotation.Nullable;

public class Endpoint {

    private final SlotApiConnection slotApiConnection;
    private final SlotInstance slot;
    private final SlotOwner slotOwner;
    private final LifeStatus lifeStatus;

    private boolean invalidated;
    private final Runnable onInvalidate;

    Endpoint(SlotApiConnection slotApiConnection, SlotInstance slot, SlotOwner slotOwner,
             LifeStatus lifeStatus, Runnable onInvalidate)
    {
        this.slotApiConnection = slotApiConnection;
        this.slotOwner = slotOwner;
        this.slot = slot;
        this.lifeStatus = lifeStatus;
        this.invalidated = false;
        this.onInvalidate = onInvalidate;
    }

    @Nullable
    public SlotApiConnection getSlotApiConnection() {
        if (invalidated) {
            return null;
        }
        return slotApiConnection;
    }

    public URI uri() {
        return slot.uri();
    }

    public String channelId() {
        return slot.channelId();
    }

    public SlotInstance slot() {
        return slot;
    }

    public SlotOwner slotOwner() {
        return slotOwner;
    }

    public Slot.Direction slotDirection() {
        return slot.spec().direction();
    }

    public LifeStatus status() {
        return lifeStatus;
    }

    public void invalidate() {
        invalidated = true;
        onInvalidate.run();
    }

    public enum SlotOwner {
        WORKER,
        PORTAL,
    }

    public enum LifeStatus {
        BINDING,
        BOUND,
        UNBINDING,
    }
}
