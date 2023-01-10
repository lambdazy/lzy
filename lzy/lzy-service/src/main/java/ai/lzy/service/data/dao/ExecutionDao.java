package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LMST;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface ExecutionDao {
    void create(String userId, String executionId, String storageType, LMST.StorageConfig storageConfig,
                @Nullable TransactionHandle transaction) throws SQLException;

    void delete(String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    void updateStdChannelIds(String executionId, String stdoutChannelId, String stderrChannelId,
                             @Nullable TransactionHandle transaction) throws SQLException;

    void updateAllocatorSession(String executionId, String sessionId, String portalId,
                                @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateAllocateOperationData(String executionId, String opId, String vmId,
                                     @Nullable TransactionHandle transaction) throws SQLException;

    void updateAllocatedVmAddress(String executionId, String vmAddress, String fsAddress,
                                  @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateFinishData(String userId, String executionId, Status status, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setDeadExecutionStatus(String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    void saveSlots(String executionId, Set<String> slotsUri, @Nullable TransactionHandle transaction)
        throws SQLException;

    void saveChannels(Map<String, String> slot2channel, @Nullable TransactionHandle transaction) throws SQLException;

    Set<String> retainExistingSlots(Set<String> slotsUri) throws SQLException;

    Set<String> retainNonExistingSlots(String executionId, Set<String> slotsUri) throws SQLException;

    Map<String, String> findChannels(Set<String> slotsUri) throws SQLException;

    @Nullable
    default String getPortalAddress(String executionId) throws SQLException {
        var desc = getPortalDescription(executionId);
        if (desc != null) {
            return desc.vmAddress().toString();
        }
        return null;
    }

    @Nullable
    LMST.StorageConfig getStorageConfig(String executionId) throws SQLException;

    @Nullable
    PortalDescription getPortalDescription(String executionId) throws SQLException;

    @Nullable
    String getAllocatorSession(String executionId) throws SQLException;

    @Nullable
    String getExpiredExecution() throws SQLException;
}
