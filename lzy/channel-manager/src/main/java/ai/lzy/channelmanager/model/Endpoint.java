package ai.lzy.channelmanager.model;

import ai.lzy.model.slot.Slot;
import ai.lzy.model.slot.SlotInstance;

import java.net.URI;

public class Endpoint {

    private final SlotInstance slot;
    private final SlotOwner slotOwner;
    private final LifeStatus lifeStatus;

    public Endpoint(SlotInstance slot, SlotOwner slotOwner, LifeStatus lifeStatus)
    {
        this.slotOwner = slotOwner;
        this.slot = slot;
        this.lifeStatus = lifeStatus;
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

    public boolean isActive() {
        return switch (lifeStatus) {
            case BINDING, BOUND -> true;
            case UNBINDING -> false;
        };
    }

    public enum SlotOwner {
        WORKER,
        PORTAL,
    }

    public enum LifeStatus {
        BINDING,
        BOUND,
        UNBINDING
    }
}
