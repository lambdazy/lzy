package ai.lzy.model.slot;

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

    public String shortDesc() {
        return taskId + '/' + spec.name();
    }
}
