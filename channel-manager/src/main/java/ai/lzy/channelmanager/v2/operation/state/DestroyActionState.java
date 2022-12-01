package ai.lzy.channelmanager.v2.operation.state;

import java.util.HashSet;

public record DestroyActionState(
    HashSet<String> toDestroyChannels,
    HashSet<String> destroyedChannels
) {
    public DestroyActionState setDestroyed(String channelId) {
        toDestroyChannels.remove(channelId);
        destroyedChannels.add(channelId);
        return this;
    }
}
