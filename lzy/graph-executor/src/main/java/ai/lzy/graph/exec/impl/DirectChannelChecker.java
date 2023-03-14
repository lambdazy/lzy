package ai.lzy.graph.exec.impl;

import ai.lzy.graph.algo.GraphBuilder;
import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.exec.ChannelChecker;
import ai.lzy.graph.model.TaskExecution;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;

import java.util.Map;

public class DirectChannelChecker implements ChannelChecker {
    private final SchedulerApi api;
    private final Map<String, TaskExecution> taskDescIdToTaskExec;
    private final String workflowId;

    public DirectChannelChecker(SchedulerApi api,
                                Map<String, TaskExecution> taskDescIdToTaskExec, String workflowId)
    {
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
        final TaskStatus status = api.status(workflowId, exec.id());
        return status != null && status.hasSuccess();
    }
}
