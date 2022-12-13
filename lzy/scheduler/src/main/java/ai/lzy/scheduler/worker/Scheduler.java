package ai.lzy.scheduler.worker;

import ai.lzy.model.TaskDesc;
import ai.lzy.scheduler.task.Task;
import io.grpc.StatusException;

import java.util.List;

public interface Scheduler {
    Task execute(String workflowId, String workflowName, String userId, TaskDesc taskDesc) throws StatusException;
    Task stopTask(String workflowId, String taskId, String issue) throws StatusException;
    Task status(String workflowId, String taskId) throws StatusException;
    void killAll(String workflowName, String issue) throws StatusException;
    List<Task> list(String workflow) throws StatusException;

    void start();
    void terminate();
    void awaitTermination() throws InterruptedException;
}