package ai.lzy.graph.db;

import ai.lzy.graph.model.TaskOperation;
import ai.lzy.graph.model.TaskState;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TaskDao {
    void createTasks(List<TaskState> tasks, @Nullable TransactionHandle transaction) throws SQLException;

    void updateTask(TaskState task, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    TaskState getTaskById(String taskId) throws SQLException;

    List<TaskState> getTasksByGraph(String graphId) throws SQLException;

    List<TaskState> getTasksByInstance(String instanceId) throws SQLException;

    void createTaskOperation(TaskOperation taskOperation, @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateTaskOperation(TaskOperation taskOperation, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    TaskOperation getTaskOperationById(String taskOperationId) throws SQLException;

    List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws SQLException;
}
