package ai.lzy.graph.test;

import ai.lzy.graph.api.SchedulerApi;
import ai.lzy.graph.model.TaskDescription;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;
import io.grpc.Status;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

//@Primary
//@Requires(env = "test-mock")
public class SchedulerApiMock implements SchedulerApi {
    private final Map<String, TaskStatus> statusByTaskId = new ConcurrentHashMap<>();
    private final Set<String> exceptions = ConcurrentHashMap.newKeySet();
    private final OnExecute callback;

    public SchedulerApiMock() {
        this.callback = (a, b, sch) -> {
            sch.changeStatus(b.id(), TaskStatus.newBuilder()
                .setTaskId(b.id())
                .setExecuting(TaskStatus.Executing.newBuilder().build())
                .build()
            );
            return b.id();
        };
    }

    public SchedulerApiMock(OnExecute callback) {
        this.callback = callback;
    }

    @Override
    public TaskStatus execute(String userId, String wfName, String workflowId, TaskDescription tasks) {
        String taskId = callback.call(workflowId, tasks, this);
        return status(workflowId, taskId);
    }

    @Override
    @Nullable
    public TaskStatus status(String workflowId, String taskId) {
        if (exceptions.contains(taskId)) {
            throw Status.INTERNAL.asRuntimeException();
        }
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

    public void raiseException(String taskId) {
        exceptions.add(taskId);
    }

    public static final TaskStatus EXECUTING = TaskStatus.newBuilder()
        .setExecuting(TaskStatus.Executing.newBuilder().build()).build();
    public static final TaskStatus ERROR = TaskStatus.newBuilder()
        .setError(TaskStatus.Error.newBuilder().build()).build();
    public static final TaskStatus COMPLETED = TaskStatus.newBuilder()
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
