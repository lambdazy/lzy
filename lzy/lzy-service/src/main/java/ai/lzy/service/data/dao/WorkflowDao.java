package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;


public interface WorkflowDao {
    record WorkflowInfo(
        String workflowName,
        String userId
    ) {}

    /**
     * Returns previous active execution id or null.
     */
    @Nullable
    String upsert(String ownerId, String workflowName, String executionId, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    WorkflowInfo findWorkflowBy(String executionId) throws SQLException;

    void setActiveExecutionToNull(String userId, String workflowName, String executionId,
                                  @Nullable TransactionHandle transaction) throws SQLException;

    void setActiveExecutionToNull(String userId, String executionId, @Nullable TransactionHandle transaction)
        throws SQLException;
}
