package ai.lzy.scheduler.db;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TaskDao {

    @Nullable
    String getAllocatorSession(String workflowName, String userId, @Nullable TransactionHandle tx) throws SQLException;

    void insertAllocatorSession(String workflowName, String userId,
                                String sessionId, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    TaskDesc getTaskDesc(String taskId, String executionId, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    TaskDesc getTaskDesc(String operationId, @Nullable TransactionHandle tx) throws SQLException;

    void insertTaskDesc(TaskDesc desc, @Nullable TransactionHandle tx) throws SQLException;

    List<TaskDesc> listTasks(String executionId, @Nullable TransactionHandle tx) throws SQLException;

    List<TaskDesc> listByWfName(String wfName, String userId, @Nullable TransactionHandle tx) throws SQLException;

    record TaskDesc(
        String taskId,
        String executionId,
        String workflowName,
        String userId,
        String operationId,
        String operationName
    ) {}
}
