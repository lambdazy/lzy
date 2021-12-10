package ru.yandex.cloud.ml.platform.lzy.server.task;

import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.configs.TasksConfig;
import ru.yandex.cloud.ml.platform.lzy.server.kuber.task.KuberTask;
import ru.yandex.cloud.ml.platform.lzy.server.local.task.LocalDockerTask;
import ru.yandex.cloud.ml.platform.lzy.server.local.task.LocalProcessTask;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

@Singleton
public class TaskFactory {
    private static final Logger LOG = LogManager.getLogger(TaskFactory.class);

    private final TasksConfig tasksConfig;

    public TaskFactory(TasksConfig tasksConfig) {
        this.tasksConfig = tasksConfig;
    }

    public Task createTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments, SnapshotMeta meta, ChannelsManager channels, URI serverURI) {
        LOG.info("Task type={}", tasksConfig.taskType());
        switch (tasksConfig.taskType()) {
            case LOCAL_DOCKER:
                return new LocalDockerTask(owner, tid, workload, assignments, meta, channels, serverURI);
            case KUBER:
                return new KuberTask(owner, tid, workload, assignments, meta, channels, serverURI);
            case LOCAL_PROCESS:
            default:
                return new LocalProcessTask(owner, tid, workload, assignments, meta, channels, serverURI, tasksConfig.localProcessTaskConfig());
        }
    }
}
