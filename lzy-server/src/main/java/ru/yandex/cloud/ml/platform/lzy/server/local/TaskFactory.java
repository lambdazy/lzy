package ru.yandex.cloud.ml.platform.lzy.server.local;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class TaskFactory {
    public static Task createTask(String owner, UUID tid, Zygote workload, Map<Slot, String> assignments, ChannelsManager channels, URI serverURI) {
        final String taskType = System.getProperty("lzy.server.task.type", "default");
        switch (taskType) {
            case "local-docker":
                return new LocalDockerTask(owner, tid, workload, assignments, channels, serverURI);
            case "kuber":
                return new KuberTask(owner, tid, workload, assignments, channels, serverURI);
            case "local-process":
            default:
                return new LocalProcessTask(owner, tid, workload, assignments, channels, serverURI);
        }
    }
}
