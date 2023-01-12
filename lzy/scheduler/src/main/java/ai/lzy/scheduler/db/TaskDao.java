package ai.lzy.scheduler.db;

import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.task.Task;

import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

public interface TaskDao {
    TaskState insert(
        String id, String executionId, String workflowName,
        String userId, TaskDesc taskDesc, @Nullable TransactionHandle tx
    ) throws SQLException;

    @Nullable
    TaskState get(String taskId, String executionId, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    TaskState getForAllocation(String taskId, String executionId, @Nullable TransactionHandle tx) throws SQLException;

    void updateLastActivityTime(String taskId, @Nullable TransactionHandle tx) throws SQLException;

    void updateAllocatorData(String taskId, String execId, String opId, String vmId,
                             @Nullable TransactionHandle tx) throws SQLException;

    void updateWorkerData(String taskId, String execId, String workerAddress, String workerOperationId,
                          @Nullable TransactionHandle tx) throws SQLException;

    void updateExecutionCompleted(String taskId, String execId, int rc, String description,
                                  @Nullable TransactionHandle tx) throws SQLException;

    void fail(String taskId, String execId,
              int rc, String description, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    String getAllocatorSession(String workflowName, String userId, @Nullable TransactionHandle tx) throws SQLException;

    void insertAllocatorSession(String workflowName, String userId,
                                String sessionId, @Nullable TransactionHandle tx) throws SQLException;
}
