package ai.lzy.model.slot;

import java.net.URI;
import javax.annotation.Nullable;

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
