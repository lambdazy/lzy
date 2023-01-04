package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.PortalStatus;
import ai.lzy.v1.common.LMST;
import io.grpc.Status;

import java.sql.SQLException;
import java.sql.Timestamp;
import javax.annotation.Nullable;


public interface WorkflowDao {
    default void create(String executionId, String userId, String workflowName, String storageType,
                        LMST.StorageConfig storageConfig) throws AlreadyExistsException, SQLException
    {
        create(executionId, userId, workflowName, storageType, storageConfig, null);
    }

    void create(String executionId, String userId, String workflowName, String storageType,
                LMST.StorageConfig storageConfig, @Nullable TransactionHandle transaction)
        throws AlreadyExistsException, SQLException;

    boolean doesActiveExecutionExists(String userId, String workflowName, String executionId) throws SQLException;

    default void setDeadExecutionStatus(String executionId) throws SQLException {
        setDeadExecutionStatus(executionId, null);
    }

    void setDeadExecutionStatus(String executionId,
                                @Nullable TransactionHandle transaction) throws SQLException;

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

    default void updateAllocatorSession(String executionId, String sessionId, String portalId) throws SQLException {
        updateAllocatorSession(executionId, sessionId, portalId, null);
    }

    void updateAllocatorSession(String executionId, String sessionId, String portalId,
                                @Nullable TransactionHandle transaction)
        throws SQLException;

    default void updateAllocateOperationData(String executionId, String opId, String vmId) throws SQLException {
        updateAllocateOperationData(executionId, opId, vmId, null);
    }

    void updateAllocateOperationData(String executionId, String opId, String vmId,
                                     @Nullable TransactionHandle transaction) throws SQLException;

    default void updateAllocatedVmAddress(String executionId, String vmAddress, String fsAddress) throws SQLException {
        updateAllocatedVmAddress(executionId, vmAddress, fsAddress, null);
    }

    void updateAllocatedVmAddress(String executionId, String vmAddress, String fsAddress,
                                  @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    Status getExecutionErrorStatus(String executionId) throws SQLException;

    void updateFinishData(String workflowName, String executionId, Timestamp finishedAt,
                          @Nullable String finishedWithError, @Nullable Integer finishedErrorCode,
                          @Nullable TransactionHandle transaction)
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

    default String getPortalAddress(String executionId) throws SQLException {
        var desc = getPortalDescription(executionId);
        if (desc == null) {
            throw new NotFoundException("Cannot obtain portal address");
        }
        return desc.vmAddress().toString();
    }

    LMST.StorageConfig getStorageConfig(String executionId) throws SQLException;

    @Nullable
    PortalDescription getPortalDescription(String executionId) throws SQLException;

    @Nullable
    String getAllocatorSession(String executionId) throws SQLException;

    String getExpiredExecution() throws SQLException;
}
