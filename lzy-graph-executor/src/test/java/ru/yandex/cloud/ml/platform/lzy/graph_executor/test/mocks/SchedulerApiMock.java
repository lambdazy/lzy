package ru.yandex.cloud.ml.platform.lzy.graph_executor.test.mocks;

import ru.yandex.cloud.ml.platform.lzy.graph_executor.api.SchedulerApi;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskDescription;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchedulerApiMock implements SchedulerApi {
    final private Map<String, Tasks.TaskProgress> statusByTaskId = new ConcurrentHashMap<>();
    private final OnExecute callback;

    public SchedulerApiMock(OnExecute callback) {
        this.callback = callback;
    }

    public SchedulerApiMock() {
        this.callback = (a, b, c) -> "";
    }

    @Override
    public Tasks.TaskProgress execute(String workflowId, TaskDescription tasks) {
        String taskId = callback.call(workflowId, tasks, this);
        return status(workflowId, taskId);
    }

    @Override
    @Nullable
    public Tasks.TaskProgress status(String workflowId, String taskId) {
        return statusByTaskId.get(taskId);
    }

    @Override
    public Tasks.TaskProgress kill(String workflowId, String taskId) {
        changeStatus(taskId, ERROR);
        return status(workflowId, taskId);
    }

    public synchronized void changeStatus(String taskId, Tasks.TaskProgress status) {
        statusByTaskId.put(taskId, status);
        notifyAll();
    }

    public final static Tasks.TaskProgress QUEUE = Tasks.TaskProgress.newBuilder()
        .setStatus(Tasks.TaskProgress.Status.QUEUE).build();
    public final static Tasks.TaskProgress EXECUTING = Tasks.TaskProgress.newBuilder()
        .setStatus(Tasks.TaskProgress.Status.EXECUTING).build();
    public final static Tasks.TaskProgress ERROR = Tasks.TaskProgress.newBuilder()
            .setStatus(Tasks.TaskProgress.Status.ERROR).build();
    public final static Tasks.TaskProgress COMPLETED = Tasks.TaskProgress.newBuilder()
            .setStatus(Tasks.TaskProgress.Status.SUCCESS).build();

    public interface OnExecute {
        String call(String workflowId, TaskDescription tasks, SchedulerApiMock scheduler);
    }

    public synchronized void waitForStatus(String taskId, Tasks.TaskProgress.Status status) throws InterruptedException {
        while (statusByTaskId.get(taskId) == null || statusByTaskId.get(taskId).getStatus() != status) {
            this.wait();
        }
    }
}
