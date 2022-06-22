package ru.yandex.cloud.ml.platform.lzy.graph.exec;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import ru.yandex.cloud.ml.platform.lzy.graph.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.impl.DirectChannelChecker;
import ru.yandex.cloud.ml.platform.lzy.graph.model.ChannelDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskExecution;

@Singleton
public class ChannelCheckerFactory {

    private final SchedulerApi api;

    @Inject
    public ChannelCheckerFactory(SchedulerApi api) {
        this.api = api;
    }

    public ChannelChecker checker(
        Map<String, TaskExecution> taskDescIdToTaskExec,
        String workflowId,
        ChannelDescription channel
    ) {
        return switch (channel.type()) {
            case DIRECT -> new DirectChannelChecker(api, taskDescIdToTaskExec, workflowId);
        };
    }
}
