package ai.lzy.graph.test.mocks;

import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.priv.v2.SchedulerApi.TaskStatus;
import ai.lzy.priv.v2.Tasks;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchedulerApiMock implements SchedulerApi {
    final private Map<String, TaskStatus> statusByTaskId = new ConcurrentHashMap<>();
    private final OnExecute callback;

    public SchedulerApiMock(OnExecute callback) {
        this.callback = callback;
    }

    public SchedulerApiMock() {
        this.callback = (a, b, c) -> "";
    }

    @Override
    public TaskStatus execute(String workflowId, TaskDescription tasks) {
        String taskId = callback.call(workflowId, tasks, this);
        return status(workflowId, taskId);
    }

    @Override
    @Nullable
    public TaskStatus status(String workflowId, String taskId) {
        return statusByTaskId.get(taskId);
    }

    @Override
    public TaskStatus kill(String workflowId, String taskId) {
        changeStatus(taskId, ERROR);
        return status(workflowId, taskId);
    }

    public void changeStatus(String taskId, TaskStatus status) {
        statusByTaskId.put(taskId, status);
    }

    public final static TaskStatus QUEUE = TaskStatus.newBuilder()
        .setQueue(TaskStatus.Queue.newBuilder().build()).build();
    public final static TaskStatus EXECUTING = TaskStatus.newBuilder()
        .setExecuting(TaskStatus.Executing.newBuilder().build()).build();
    public final static TaskStatus ERROR = TaskStatus.newBuilder()
            .setError(TaskStatus.Error.newBuilder().build()).build();
    public final static TaskStatus COMPLETED = TaskStatus.newBuilder()
            .setSuccess(TaskStatus.Success.newBuilder().build()).build();

    public interface OnExecute {
        String call(String workflowId, TaskDescription tasks, SchedulerApiMock scheduler);
    }

    public void waitForStatus(String taskId, TaskStatus.StatusCase status) throws InterruptedException {
        while (statusByTaskId.get(taskId) == null || statusByTaskId.get(taskId).getStatusCase() != status) {
            Thread.sleep(10);
        }
    }
}
