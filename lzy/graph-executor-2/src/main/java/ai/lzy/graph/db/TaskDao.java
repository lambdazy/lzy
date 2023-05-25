package ai.lzy.graph.db;

import ai.lzy.graph.model.Task;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TaskDao {
    void createOrUpdateTask(Task task, @Nullable TransactionHandle transaction) throws SQLException;
    Task getTaskById(String taskId) throws DaoException;
    List<Task> getTasksByGraph(String graphId) throws DaoException;
    List<Task> getTasksByInstance(String instanceId) throws DaoException;

    void createOrUpdateTaskOperation(TaskOperation taskOperation, @Nullable TransactionHandle transaction)
        throws SQLException;
    TaskOperation getTaskOperationById(String taskOperationId) throws DaoException;
    List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws DaoException;
}
