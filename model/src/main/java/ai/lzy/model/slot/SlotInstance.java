package ai.lzy.model.slot;

import ai.lzy.model.slot.Slot;
import java.net.URI;

public record SlotInstance(
    Slot spec,
    String taskId,
    String channelId,
    URI uri
) {
    public String name() {
        return spec.name();
    }
}
