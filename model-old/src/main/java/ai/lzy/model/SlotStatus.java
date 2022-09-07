package ai.lzy.model;

import javax.annotation.Nullable;
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
