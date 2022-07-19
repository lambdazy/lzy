package ai.lzy.model;

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
