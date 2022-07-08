package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.task.Task;
import io.grpc.StatusException;

import java.util.List;

public interface Scheduler {
    Task execute(String workflowId, String workflowName, TaskDesc taskDesc) throws StatusException;
    Task stopTask(String workflowId, String taskId, String issue) throws StatusException;
    Task status(String workflowId, String taskId) throws StatusException;
    void killAll(String workflowName, String issue) throws StatusException;
    List<Task> list(String workflow) throws StatusException;

    void terminate();
    void awaitTermination() throws InterruptedException;
}
