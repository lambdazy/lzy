package ai.lzy.graph.api;

import ai.lzy.graph.model.TaskDescription;
import ai.lzy.priv.v2.Tasks;

import javax.annotation.Nullable;

public interface SchedulerApi {

    /**
     * Schedule task
     * @param workflowId id of workflow to execute task in
     * @param task descriptions of task to execute
     * @return status of task that was registered
     */
    Tasks.TaskProgress execute(String workflowId, TaskDescription task);

    /**
     * Get status of task by id
     * @param workflowId id of workflow of task
     * @param taskId task id
     * @return current status of task, null if not found
     */
    @Nullable
    Tasks.TaskProgress status(String workflowId, String taskId);

    /**
     * Send kill to task by id
     * @param workflowId id of workflow of task
     * @param taskId task id
     * @return current status of task
     */
    Tasks.TaskProgress kill(String workflowId, String taskId);
}
