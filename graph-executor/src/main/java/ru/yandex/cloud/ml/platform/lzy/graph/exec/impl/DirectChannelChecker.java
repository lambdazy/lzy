package ru.yandex.cloud.ml.platform.lzy.graph.exec.impl;

import java.util.Map;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.GraphBuilder;
import ru.yandex.cloud.ml.platform.lzy.graph.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph.exec.ChannelChecker;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskExecution;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.TaskProgress.Status;

public class DirectChannelChecker implements ChannelChecker {
    private final SchedulerApi api;
    private final Map<String, TaskExecution> taskDescIdToTaskExec;
    private final String workflowId;

    public DirectChannelChecker(SchedulerApi api,
                                Map<String, TaskExecution> taskDescIdToTaskExec, String workflowId) {
        this.api = api;
        this.taskDescIdToTaskExec = taskDescIdToTaskExec;
        this.workflowId = workflowId;
    }

    @Override
    public boolean ready(GraphBuilder.ChannelEdge edge) {
        final TaskExecution exec = taskDescIdToTaskExec.get(edge.input().description().id());
        if (exec == null) {
            return false;
        }
        final Tasks.TaskProgress progress = api.status(workflowId, exec.id());
        return progress != null && progress.getStatus() == Status.SUCCESS;
    }
}
