package ai.lzy.channelmanager.v2.model;

import ai.lzy.model.slot.Slot;

public record Connection(Endpoint sender, Endpoint receiver, LifeStatus status) {

    public static Connection of(Endpoint e1, Endpoint e2) {
        if (e1.slotDirection() == Slot.Direction.OUTPUT && e2.slotDirection() == Slot.Direction.INPUT) {
            return new Connection(e1, e2, LifeStatus.CONNECTING);
        }
        if (e1.slotDirection() == Slot.Direction.INPUT && e2.slotDirection() == Slot.Direction.OUTPUT) {
            return new Connection(e2, e1, LifeStatus.CONNECTING);
        }
        throw new IllegalArgumentException("Invalid endpoint directions");
    }

    public enum LifeStatus {
        CONNECTING,
        CONNECTED,
        DISCONNECTING,
    }

}
