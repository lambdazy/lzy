package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.task.Task;
import java.util.List;
import javax.annotation.Nullable;

public interface TaskDao {
    Task create(String workflowId, String workflowName, TaskDesc taskDesc) throws DaoException;

    @Nullable
    Task get(String workflowId, String taskId) throws DaoException;

    List<Task> filter(TaskState.Status status) throws DaoException;

    List<Task> list(String workflowId) throws DaoException;

    void update(Task state) throws DaoException;
}
