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

    public URI getUri() {
        return slot.uri();
    }

    public String getChannelId() {
        return slot.channelId();
    }

    public SlotInstance getSlot() {
        return slot;
    }

    public SlotOwner getSlotOwner() {
        return slotOwner;
    }

    public Slot.Direction getSlotDirection() {
        return slot.spec().direction();
    }

    public LifeStatus getStatus() {
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
