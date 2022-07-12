package ai.lzy.server.task;

import ai.lzy.model.Signal;
import ai.lzy.server.configs.ServerConfig;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.Zygote;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.server.Authenticator;
import ai.lzy.server.ChannelsManager;
import ai.lzy.server.TasksManager;

import static ai.lzy.priv.v2.Tasks.TaskProgress.Status.ERROR;
import static ai.lzy.priv.v2.Tasks.TaskProgress.Status.SUCCESS;

@Singleton
public class InMemTasksManager implements TasksManager {
    private static final Logger LOG = LogManager.getLogger(InMemTasksManager.class);
    protected final URI serverURI;
    private final ChannelsManager channels;
    private final Map<String, Task> tasks = new ConcurrentHashMap<>();

    private final Map<String, List<Task>> userTasks = new ConcurrentHashMap<>();
    private final Map<Task, Task> parents = new ConcurrentHashMap<>();
    private final Map<Task, String> owners = new ConcurrentHashMap<>();
    private final Map<Task, List<Task>> children = new ConcurrentHashMap<>();

    private final Map<Task, List<ChannelSpec>> taskChannels = new ConcurrentHashMap<>();
    private final Map<String, List<ChannelSpec>> userChannels = new ConcurrentHashMap<>();

    private final Map<String, Map<Slot, ChannelSpec>> userSlots = new ConcurrentHashMap<>();

    public InMemTasksManager(ServerConfig serverConfig, ChannelsManager channels) {
        this.serverURI = serverConfig.getServerUri();
        this.channels = channels;
    }

    @Override
    public ChannelSpec createChannel(String uid, Task parent, ChannelSpec channelSpec) {
        final ChannelSpec channel = channels.create(channelSpec);
        if (channel == null) {
            return null;
        }
        if (parent != null) {
            taskChannels.computeIfAbsent(parent, task -> new ArrayList<>()).add(channel);
        } else {
            userChannels.computeIfAbsent(uid, user -> new ArrayList<>()).add(channel);
        }
        return channel;
    }

    @Override
    public SlotStatus[] connected(ChannelSpec channel) {
        return channels.connected(channel);
    }

    @Override
    public String owner(String tid) {
        return owners.get(task(tid));
    }

    @Override
    public Map<Slot, ChannelSpec> slots(String user) {
        return userSlots.getOrDefault(user, Map.of());
    }

    @Override
    public void addUserSlot(String user, Slot slot, ChannelSpec channel) {
        userSlots.computeIfAbsent(user, u -> new ConcurrentHashMap<>()).put(slot, channel);
    }

    @Override
    public boolean removeUserSlot(String user, Slot slot) {
        return userSlots.getOrDefault(user, Map.of()).remove(slot) != null;
    }

    @Override
    public void destroyUserChannels(String user) {
        userChannels.getOrDefault(user, List.of()).forEach(channels::destroy);
        userChannels.remove(user);
    }

    @Override
    public Stream<ChannelSpec> cs() {
        return channels.channels();
    }

    @Override
    public Task start(String uid, Task parent, Zygote workload, Map<Slot, String> assignments, Authenticator auth) {
        final Task task = new TaskImpl(
            uid, "task_" + UUID.randomUUID(), workload, assignments,
            channels, serverURI
        );
        tasks.put(task.tid(), task);
        if (parent != null) {
            children.computeIfAbsent(parent, t -> new ArrayList<>()).add(task);
            parents.put(task, parent);
        }
        userTasks.computeIfAbsent(uid, user -> new ArrayList<>()).add(task);
        owners.put(task, uid);
        task.onProgress(progress -> {
            LOG.info("InMemTaskManager::progress " + JsonUtils.printRequest(progress));
            if (!EnumSet.of(ERROR, SUCCESS).contains(progress.getStatus())) // task is not concluded
                return;
            if (tasks.remove(task.tid()) == null) // idempotence support
                return;

            children.getOrDefault(task, List.of()).forEach(child -> child.signal(Signal.TERM));
            children.remove(task);
            final Task removedTask = parents.remove(task);
            if (removedTask != null) {
                children.getOrDefault(removedTask, new ArrayList<>()).remove(task);
            }
            taskChannels.getOrDefault(task, List.of()).forEach(channels::destroy);
            taskChannels.remove(task);
            userTasks.getOrDefault(owners.remove(task), List.of()).remove(task);
        });
        return task;
    }

    @Override
    public Task task(String tid) {
        LOG.info("Resolving task tid=" + tid);
        return tasks.get(tid);
    }

    @Override
    public Stream<Task> ps() {
        return tasks.values().stream();
    }

    @Override
    public ChannelSpec channel(String chName) {
        return channels.channels().filter(ch -> ch.name().equals(chName)).findFirst().orElse(null);
    }
}
