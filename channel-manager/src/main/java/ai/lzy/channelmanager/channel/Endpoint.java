package ai.lzy.channelmanager.channel;

import ai.lzy.model.Slot;
import ai.lzy.model.SlotInstance;
import ai.lzy.model.SlotStatus;
import java.net.URI;
import javax.annotation.Nullable;

public interface Endpoint {
    URI uri();

    Slot slotSpec();

    SlotInstance slotInstance();

    String taskId();

    int connect(SlotInstance slotInstance);

    @Nullable
    SlotStatus status();

    int disconnect();

    int destroy();

    void invalidate();

    boolean isInvalid();
}
