package ai.lzy.scheduler.test.mocks;

import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.task.TaskImpl;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TaskDaoMock implements TaskDao {
    private final Map<String, TaskState> storage = new ConcurrentHashMap<>();

    @Override
    public Task create(String workflowId, String workflowName, TaskDesc taskDesc) {
        final String taskId = UUID.randomUUID().toString();
        final TaskState state = new TaskState(
            taskId, workflowId, workflowName, taskDesc, TaskState.Status.QUEUE,
            null, null, null
        );
        storage.put(taskId, state);
        return new TaskImpl(state, this);
    }

    @Nullable
    @Override
    public Task get(String taskId) {
        final TaskState state = storage.get(taskId);
        if (state == null) {
            return null;
        }
        return new TaskImpl(state, this);
    }

    @Override
    public List<Task> filter(TaskState.Status status) {
        return storage.values()
            .stream()
            .filter(t -> t.status().equals(status))
            .map(t -> new TaskImpl(t, this))
            .map(t -> (Task) t)
            .toList();
    }

    @Override
    public List<Task> list(String workflowId) throws DaoException {
        return storage.values().stream()
            .filter(t -> t.workflowId().equals(workflowId))
            .map(t -> (Task)new TaskImpl(t, this))
            .toList();
    }

    @Override
    public void update(Task state) {
        final TaskState newState = new TaskState(state.taskId(), state.workflowId(), state.workflowName(),
            state.description(), state.status(), state.rc(), state.errorDescription(), state.servantId());
        storage.put(state.taskId(), newState);
    }
}
