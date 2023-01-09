package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;


public interface WorkflowDao {
    /**
     * Returns previous active execution id or null.
     */
    @Nullable
    String upsert(String ownerId, String workflowName, String executionId, @Nullable TransactionHandle transaction)
        throws SQLException;

    /**
     * Returns user id and name of workflow with active execution.
     */
    @Nullable
    String[] findWorkflowBy(String executionId) throws SQLException;

    void setActiveExecutionToNull(String userId, String workflowName, String executionId,
                                  @Nullable TransactionHandle transaction) throws SQLException;
}
