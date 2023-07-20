package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;


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

    boolean resetActiveExecutionById(String wfName, String activeExecId, Instant cacheSessionDeadline,
                                     @Nullable TransactionHandle transaction) throws SQLException;

    void resetActiveExecution(String userId, String wfName, Instant cacheSessionDeadline,
                              @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    String acquireCurrentAllocatorSession(String userId, String wfName) throws SQLException;

    /**
     * @return existing allocator session or null
     *         If allocator session already exists this method do nothing and returns existing sid.
     */
    @Nullable
    String setAllocatorSessionId(String userId, String wfName, String sessionId) throws SQLException;

    boolean cleanAllocatorSessionId(String userId, String wfName, String sessionId, @Nullable TransactionHandle tx)
        throws SQLException;

    record OutdatedAllocatorSession(
        String userId,
        String wfName,
        String allocSessionId
    ) {}

    List<OutdatedAllocatorSession> listOutdatedAllocatorSessions(int limit) throws SQLException;
}
