package ai.lzy.server.task;

import static ai.lzy.v1.deprecated.LzyTask.TaskProgress.Status.ERROR;
import static ai.lzy.v1.deprecated.LzyTask.TaskProgress.Status.SUCCESS;

import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.model.slot.Slot;
import ai.lzy.model.deprecated.Zygote;
import ai.lzy.server.Authenticator;
import ai.lzy.server.TasksManager;
import ai.lzy.model.Signal;
import ai.lzy.server.configs.ServerConfig;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class InMemTasksManager implements TasksManager {
    private static final Logger LOG = LogManager.getLogger(InMemTasksManager.class);
    protected final URI serverURI;
    private final Map<String, Task> tasks = new ConcurrentHashMap<>();

    private final Map<String, List<Task>> userTasks = new ConcurrentHashMap<>();
    private final Map<Task, Task> parents = new ConcurrentHashMap<>();
    private final Map<Task, String> owners = new ConcurrentHashMap<>();
    private final Map<Task, List<Task>> children = new ConcurrentHashMap<>();

    public InMemTasksManager(ServerConfig serverConfig) {
        this.serverURI = serverConfig.getServerUri();
    }

    @Override
    public String owner(String tid) {
        return owners.get(task(tid));
    }

    @Override
    public Task start(String uid, Task parent, Zygote workload, Map<Slot, String> assignments, Authenticator auth) {
        final Task task = new TaskImpl(
            uid, "task_" + UUID.randomUUID(), workload, assignments, serverURI
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
    public Stream<Task> tasks() {
        return tasks.values().stream();
    }
}
