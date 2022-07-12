package ai.lzy.server;

import ai.lzy.server.task.Task;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.Zygote;
import ai.lzy.model.channel.ChannelSpec;

import java.util.Map;
import java.util.stream.Stream;

public interface TasksManager {
    Task task(String tid);

    Task start(String uid, Task parent, Zygote workload, Map<Slot, String> assignments, Authenticator token);

    Stream<Task> ps();

    Stream<ChannelSpec> cs();

    ChannelSpec channel(String chName);

    ChannelSpec createChannel(String uid, Task parent, ChannelSpec channelSpec);

    SlotStatus[] connected(ChannelSpec channel);

    String owner(String tid);

    Map<Slot, ChannelSpec> slots(String user);

    void addUserSlot(String user, Slot slot, ChannelSpec channel);

    boolean removeUserSlot(String user, Slot slot);

    void destroyUserChannels(String user);
}
