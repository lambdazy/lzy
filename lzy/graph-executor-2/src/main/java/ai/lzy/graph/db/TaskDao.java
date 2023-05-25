package ai.lzy.graph.db;

import ai.lzy.graph.model.Task;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TaskDao {
    void createOrUpdateTask(Task task, @Nullable TransactionHandle transaction) throws SQLException;
    Task getTaskById(String taskId) throws SQLException;
    List<Task> getTasksByGraph(String graphId) throws SQLException;
    List<Task> getTasksByInstance(String instanceId) throws SQLException;

    void createOrUpdateTaskOperation(TaskOperation taskOperation, @Nullable TransactionHandle transaction)
        throws SQLException;
    TaskOperation getTaskOperationById(String taskOperationId) throws SQLException;
    List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws SQLException;
}
