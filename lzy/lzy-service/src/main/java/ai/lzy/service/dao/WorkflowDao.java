package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;


public interface WorkflowDao {
    /**
     * Returns previous active execution id if wf with `workflowName` and `userId` already exists, otherwise null.
     */
    @Nullable
    String upsert(String userId, String wfName, String newActiveExecId, @Nullable TransactionHandle transaction)
        throws SQLException;

    boolean exists(String userId, String wfName) throws SQLException;

    @Nullable
    String getExecutionId(String userId, String wfName, @Nullable TransactionHandle transaction) throws SQLException;

    boolean setActiveExecutionIdToNull(String brokenExecId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setActiveExecutionId(String userId, String wfName, @Nullable String execId,
                              @Nullable TransactionHandle transaction) throws SQLException;
}
