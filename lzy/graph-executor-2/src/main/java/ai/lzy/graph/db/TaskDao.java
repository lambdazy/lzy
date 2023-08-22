package ai.lzy.graph.db;

import ai.lzy.graph.model.TaskState;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TaskDao {
    void createTasks(List<TaskState> tasks, @Nullable TransactionHandle transaction) throws SQLException;

    void updateTask(TaskState task, @Nullable TransactionHandle transaction) throws SQLException;

    boolean updateTask(TaskState newState, TaskState.Status expectedStatus, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    TaskState getTaskById(String taskId) throws SQLException;

    List<TaskState> loadGraphTasks(String graphId) throws SQLException;

    List<TaskState> loadActiveTasks(String instanceId) throws SQLException;
}
