package ai.lzy.kharon.workflow.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.v1.common.LMS3;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.sql.Timestamp;

import static ai.lzy.kharon.workflow.WorkflowService.PortalStatus;

public interface ExecutionDao {
    void create(String executionId, String userId, String workflowName, String storageType, LMS3.S3Locator storageData,
                @Nullable TransactionHandle transaction) throws AlreadyExistsException, SQLException;

    boolean doesActiveExecutionExists(String userId, String workflowName, String executionId,
                                      @Nullable TransactionHandle transaction) throws SQLException;

    default boolean doesActiveExecutionExists(String userId, String workflowName, String executionId)
        throws SQLException
    {
        return doesActiveExecutionExists(userId, workflowName, executionId, null);
    }

    default void updateStatus(String executionId, PortalStatus portalStatus) throws SQLException {
        updateStatus(executionId, portalStatus, null);
    }

    void updateStatus(String executionId, PortalStatus portalStatus, @Nullable TransactionHandle transaction)
        throws SQLException;

    default void updateStdChannelIds(String executionId, String stdoutChannelId, String stderrChannelId)
        throws SQLException
    {
        updateStdChannelIds(executionId, stdoutChannelId, stderrChannelId, null);
    }

    void updateStdChannelIds(String executionId, String stdoutChannelId, String stderrChannelId,
                             @Nullable TransactionHandle transaction) throws SQLException;

    default void updateAllocatorSession(String executionId, String sessionId) throws SQLException {
        updateAllocatorSession(executionId, sessionId, null);
    }

    void updateAllocatorSession(String executionId, String sessionId, @Nullable TransactionHandle transaction)
        throws SQLException;

    default void updateAllocateOperationData(String executionId, String opId, String vmId) throws SQLException {
        updateAllocateOperationData(executionId, opId, vmId, null);
    }

    void updateAllocateOperationData(String executionId, String opId, String vmId,
                                     @Nullable TransactionHandle transaction) throws SQLException;

    default void updateAllocatedVmAddress(String executionId, String vmAddress) throws SQLException {
        updateAllocatedVmAddress(executionId, vmAddress, null);
    }

    void updateAllocatedVmAddress(String executionId, String vmAddress, @Nullable TransactionHandle transaction)
        throws SQLException;

    default void updateFinishData(String workflowName, String executionId, Timestamp finishedAt,
                                  @Nullable String finishedWithError) throws SQLException
    {
        updateFinishData(workflowName, executionId, finishedAt, finishedWithError, null);
    }

    void updateFinishData(String workflowName, String executionId, Timestamp finishedAt,
                          @Nullable String finishedWithError, @Nullable TransactionHandle transaction)
        throws SQLException;

    default void updateActiveExecution(String userId, String workflowName, String oldExecutionId,
                                       @Nullable String newExecutionId) throws SQLException
    {
        updateActiveExecution(userId, workflowName, oldExecutionId, newExecutionId, null);
    }

    void updateActiveExecution(String userId, String workflowName, String oldExecutionId,
                               @Nullable String newExecutionId, @Nullable TransactionHandle transaction)
        throws SQLException;
}