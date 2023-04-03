package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.v1.common.LMST;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface ExecutionDao {
    void create(String userId, String executionId, String storageName, LMST.StorageConfig storageConfig,
                @Nullable TransactionHandle transaction) throws SQLException;

    void delete(String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    void updateStdChannelIds(String executionId, String stdoutChannelId, String stderrChannelId,
                             @Nullable TransactionHandle transaction) throws SQLException;

    void updatePortalVmAllocateSession(String executionId, String sessionId, String portalId,
                                       @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateAllocateOperationData(String executionId, String opId, String vmId,
                                     @Nullable TransactionHandle transaction) throws SQLException;

    void updatePortalVmAddress(String executionId, String vmAddress, String fsAddress,
                               @Nullable TransactionHandle transaction)
        throws SQLException;

    void updatePortalSubjectId(String executionId, String subjectId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateFinishData(String userId, String executionId, Status status, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setErrorExecutionStatus(String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    void setCompletingExecutionStatus(String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    void setCompletedExecutionStatus(String executionId, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    default String getPortalVmAddress(String executionId) throws SQLException {
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

    void setKafkaTopicDesc(String executionId, KafkaTopicDesc topicDesc,
                           @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    KafkaTopicDesc getKafkaTopicDesc(String executionId, @Nullable TransactionHandle transaction) throws SQLException;
}
