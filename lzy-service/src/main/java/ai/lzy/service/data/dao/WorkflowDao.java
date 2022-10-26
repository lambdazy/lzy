package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.service.data.PortalStatus;
import ai.lzy.v1.common.LMS3;

import java.sql.SQLException;
import java.sql.Timestamp;
import javax.annotation.Nullable;


public interface WorkflowDao {
    default void create(String executionId, String userId, String workflowName, String storageType,
                        LMS3.S3Locator storageData) throws AlreadyExistsException, SQLException
    {
        create(executionId, userId, workflowName, storageType, storageData, null);
    }

    void create(String executionId, String userId, String workflowName, String storageType, LMS3.S3Locator storageData,
                @Nullable TransactionHandle transaction) throws AlreadyExistsException, SQLException;

    boolean doesActiveExecutionExists(String userId, String workflowName, String executionId) throws SQLException;

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

    String getUserId(String executionId) throws SQLException;

    String getWorkflowName(String executionId) throws SQLException;

    String getPortalAddress(String executionId) throws SQLException;

    LMS3.S3Locator getStorageLocator(String executionId) throws SQLException;
}
