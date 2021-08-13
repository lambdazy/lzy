package ru.yandex.cloud.ml.platform.lzy.server.local;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.data.DataSchema;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.TasksManager;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class LocalTasksManager implements TasksManager {
    private final URI serverURI;
    private final ChannelsRepository channels;
    private final Map<UUID, LocalTask> tasks = new HashMap<>();

    private final Map<String, List<Task>> userTasks = new HashMap<>();
    private final Map<Task, Task> parents = new HashMap<>();
    private final Map<Task, String> owners = new HashMap<>();
    private final Map<Task, List<LocalTask>> children = new HashMap<>();

    private final Map<Task, List<Channel>> taskChannels = new HashMap<>();
    private final Map<String, List<Channel>> userChannels = new HashMap<>();

    private final Map<String, Map<Slot, Channel>> userSlots = new HashMap<>();

    public LocalTasksManager(URI serverURI, ChannelsRepository channels) {
        this.serverURI = serverURI;
        this.channels = channels;
    }

    @Override
    public Channel createChannel(String name, String uid, Task parent, DataSchema contentTypeFrom) {
        final Channel channel = channels.create(name, contentTypeFrom);
        if (channel == null)
            return null;
        if (parent != null)
            taskChannels.computeIfAbsent(parent, task -> new ArrayList<>()).add(channel);
        else
            userChannels.computeIfAbsent(uid, user -> new ArrayList<>()).add(channel);
        return channel;
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
    public Map<Slot, Channel> slots(String user) {
        return userSlots.getOrDefault(user, Map.of());
    }

    @Override
    public void addUserSlot(String user, Slot slot, Channel channel) {
        userSlots.computeIfAbsent(user, u -> new HashMap<>()).put(slot, channel);
    }

    @Override
    public boolean removeUserSlot(String user, Slot slot) {
        return userSlots.getOrDefault(user, Map.of()).remove(slot) != null;
    }

    @Override
    public Stream<Channel> cs() {
        return channels.channels();
    }

    @Override
    public Task start(String uid, Task parent, Zygote workload, Map<Slot, String> assignments, Authenticator auth, Consumer<Servant.ExecutionProgress> consumer) {
        final LocalTask task = new LocalTask(uid, UUID.randomUUID(), workload, assignments, channels, serverURI);
        tasks.put(task.tid(), task);
        if (parent != null)
            children.computeIfAbsent(parent, t -> new ArrayList<>()).add(task);
        userTasks.computeIfAbsent(uid, user -> new ArrayList<>()).add(task);
        parents.put(task, parent);
        owners.put(task, uid);
        task.onProgress(state -> {
            consumer.accept(state);
            if (!state.hasChanged() ||
                (state.getChanged().getNewState() != Servant.StateChanged.State.FINISHED &&
                    state.getChanged().getNewState() != Servant.StateChanged.State.DESTROYED))
                return;
            if (tasks.remove(task.tid()) == null) // idempotence support
                return;
            children.getOrDefault(task, List.of()).forEach(child -> child.signal(Signal.TERM));
            children.remove(task);
            children.getOrDefault(parents.remove(task), new ArrayList<>()).remove(task);
            taskChannels.getOrDefault(task, List.of()).forEach(channels::destroy);
            if (task.servant() != null)
                channels.unbindAll(task.servant());
            taskChannels.remove(task);
            userTasks.getOrDefault(owners.remove(task), List.of()).remove(task);
        });
        ForkJoinPool.commonPool().execute(() -> task.start(auth.registerTask(uid, task)));
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
    public Channel channel(String chName) {
        return channels.channels().filter(ch -> ch.name().equals(chName)).findFirst().orElse(null);
    }
}
