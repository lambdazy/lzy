package ai.lzy.graph.exec.impl;

import java.util.Map;
import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.exec.ChannelChecker;
import ai.lzy.graph.model.TaskExecution;
import ai.lzy.priv.v2.Tasks;
import ai.lzy.priv.v2.Tasks.TaskProgress.Status;

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
