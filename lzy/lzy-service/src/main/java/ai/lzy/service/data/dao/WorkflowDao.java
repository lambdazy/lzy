package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;


public interface WorkflowDao {
    /**
     * Returns previous active execution id if wf with `workflowName` and `userId` already exists, otherwise null.
     */
    @Nullable
    String upsert(String userId, String workflowName, String newActiveExecId, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    String getExecutionId(String userId, String wfName, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    WorkflowInfo findWorkflowBy(String executionId) throws SQLException;

    boolean deactivate(String userId, String workflowName, String executionId,
                       @Nullable TransactionHandle transaction) throws SQLException;

    void deactivateA(String userId, String executionId, @Nullable TransactionHandle transaction)
        throws SQLException;

    record WorkflowInfo(String workflowName, String userId) {}
}
