package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotMeta;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class TaskFactory {
    private static final Logger LOG = LogManager.getLogger(TaskFactory.class);

    public static Task createTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments, SnapshotMeta meta, ChannelsManager channels, URI serverURI) {
        final String taskType = System.getProperty("lzy.server.task.type", "default");
        LOG.info("read property lzy.server.task.type={}", taskType);
        switch (taskType) {
            case "local-docker":
                return new LocalDockerTask(owner, tid, workload, assignments, meta, channels, serverURI);
            case "kuber":
                return new KuberTask(owner, tid, workload, assignments, meta, channels, serverURI);
            case "local-process":
            default:
                return new LocalProcessTask(owner, tid, workload, assignments, meta, channels, serverURI);
        }
    }
}
