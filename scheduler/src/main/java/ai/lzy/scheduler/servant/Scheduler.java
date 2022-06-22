package ai.lzy.scheduler.servant;

import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.task.Task;
import io.grpc.StatusException;

public interface Scheduler {
    Task execute(String workflowId, TaskDesc taskDesc) throws StatusException;
    void signal(String workflowId, String taskId, int signalNumber, String issue) throws StatusException;

    void gracefulStop();
}
