package ai.lzy.model.slot;

import jakarta.annotation.Nullable;

import java.net.URI;

public interface SlotStatus {
    @Nullable
    String channelId();

    String tid();

    Slot slot();

    @Nullable
    URI connected();

    long pointer();

    State state();

    enum State {
        PREPARING, UNBOUND, OPEN, CLOSED, SUSPENDED
    }
}
