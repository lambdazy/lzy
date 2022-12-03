package ai.lzy.channelmanager.v2.operation.state;

import java.util.HashSet;

public record DestroyActionState(
    HashSet<String> toDestroyChannels,
    HashSet<String> destroyedChannels
) {
    public void setDestroyed(String channelId) {
        toDestroyChannels.remove(channelId);
        destroyedChannels.add(channelId);
    }

    public void unsetDestroyed(String channelId) {
        destroyedChannels.remove(channelId);
        toDestroyChannels.add(channelId);
    }

}
