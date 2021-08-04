package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.channel.ChannelException;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class LocalTasksManager implements TasksManager {
    private static final Logger LOG = Logger.getLogger(LocalTasksManager.class);

    private final ChannelsRepository channels = new LocalChannelsRepository();
    private final Map<UUID, LocalTask> tasks = new HashMap<>();

    private final Map<String, List<Task>> userTasks = new HashMap<>();
    private final Map<Task, Task> parents = new HashMap<>();
    private final Map<Task, String> owners = new HashMap<>();
    private final Map<Task, List<LocalTask>> children = new HashMap<>();

    private final Map<Task, List<Channel>> taskChannels = new HashMap<>();
    private final Map<String, List<Channel>> userChannels = new HashMap<>();

    @Override
    public Channel createChannel(String uid, Task parent, DataSchema contentTypeFrom) {
        final Channel channel = channels.create(contentTypeFrom);
        if (parent != null)
            taskChannels.computeIfAbsent(parent, task -> new ArrayList<>()).add(channel);
        else
            userChannels.computeIfAbsent(uid, user -> new ArrayList<>()).add(channel);
        return channel;
    }

    @Override
    public void bind(Task task, String slotName, Channel channel) throws ChannelException {
        channels.bind(channel, task, task.slot(slotName));
    }

    @Override
    public SlotStatus[] connected(Channel channel) {
        return channels.connected(channel);
    }

    @Override
    public String owner(UUID tid) {
        return owners.get(task(tid));
    }

    @Override
    public Stream<Channel> cs() {
        return channels.channels();
    }

    @Override
    public Task start(String uid, Task parent, Zygote workload, Map<String, Channel> assignments, Authenticator auth) {
        final LocalTask task = new LocalTask(UUID.randomUUID(), workload, assignments, channels);
        tasks.put(task.tid(), task);
        if (parent != null)
            children.computeIfAbsent(parent, t -> new ArrayList<>()).add(task);
        userTasks.computeIfAbsent(uid, user -> new ArrayList<>()).add(task);
        parents.put(task, parent);
        owners.put(task, uid);
        task.onStateChange(state -> {
            if (state != Task.State.FINISHED)
                return;
            if (tasks.remove(task.tid()) == null)
                return;
            children.getOrDefault(task, List.of()).forEach(child -> child.signal(Signal.KILL));
            children.remove(task);
            children.getOrDefault(parents.remove(task), List.of()).remove(task);
            taskChannels.getOrDefault(task, List.of()).forEach(channels::destroy);
            task.slots().forEach(slot -> {
                try {
                    channels.unbind(channels.bound(task, slot), task, slot);
                }
                catch (ChannelException ce) {
                    LOG.warn("Failed to unbind channel", ce);
                }
            });
            taskChannels.remove(task);
            userTasks.getOrDefault(owners.remove(task), List.of()).remove(task);
        });
        task.start(auth.registerTask(uid, task));
        return task;
    }

    @Override
    public Task task(UUID tid) {
        return tasks.get(tid);
    }

    @Override
    public Stream<Task> ps() {
        return tasks.values().stream().map(t -> t);
    }

    @Override
    public Channel channel(UUID chid) {
        return channels.channels().filter(ch -> ch.id().equals(chid)).findFirst().orElse(null);
    }
}
