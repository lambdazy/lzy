package ai.lzy.graph.db;

import ai.lzy.graph.model.Task;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TaskDao {
    void createTasks(List<Task> tasks, @Nullable TransactionHandle transaction) throws SQLException;
    void updateTask(Task task, @Nullable TransactionHandle transaction) throws SQLException;
    @Nullable
    Task getTaskById(String taskId) throws SQLException;
    List<Task> getTasksByGraph(String graphId) throws SQLException;
    List<Task> getTasksByInstance(String instanceId) throws SQLException;

    void createTaskOperation(TaskOperation taskOperation, @Nullable TransactionHandle transaction)
        throws SQLException;
    void updateTaskOperation(TaskOperation taskOperation, @Nullable TransactionHandle transaction)
        throws SQLException;
    @Nullable
    TaskOperation getTaskOperationById(String taskOperationId) throws SQLException;
    List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws SQLException;
}
