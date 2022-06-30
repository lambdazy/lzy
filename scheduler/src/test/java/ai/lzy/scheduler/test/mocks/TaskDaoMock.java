package ai.lzy.scheduler.test.mocks;

import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.task.TaskImpl;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TaskDaoMock implements TaskDao {
    private final Map<TaskKey, TaskState> storage = new ConcurrentHashMap<>();

    @Override
    public Task create(String workflowId, TaskDesc taskDesc) {
        final String taskId = UUID.randomUUID().toString();
        final TaskState state = new TaskState(
            taskId, workflowId, taskDesc, TaskState.Status.QUEUE,
            null, null, null
        );
        storage.put(new TaskKey(workflowId, taskId), state);
        return new TaskImpl(state, this);
    }

    @Nullable
    @Override
    public Task get(String workflowId, String taskId) {
        final TaskState state = storage.get(new TaskKey(workflowId, taskId));
        if (state == null) {
            return null;
        }
        return new TaskImpl(state, this);
    }

    @Override
    public List<Task> filter(TaskState.Status... statuses) {
        Set<TaskState.Status> statusSet = new HashSet<>(Arrays.asList(statuses));
        return storage.values()
            .stream()
            .filter(t -> statusSet.contains(t.status()))
            .map(t -> new TaskImpl(t, this))
            .map(t -> (Task) t)
            .toList();
    }

    @Override
    public void update(Task state) {
        final TaskState newState = new TaskState(state.taskId(), state.workflowId(),
            state.description(), state.status(), state.rc(), state.errorDescription(), state.servantId());
        storage.put(new TaskKey(state.workflowId(), state.taskId()), newState);
    }

    private record TaskKey(String workflowId, String taskId) {}
}
