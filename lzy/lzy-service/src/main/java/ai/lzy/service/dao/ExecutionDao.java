package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LMST;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface ExecutionDao {
    void create(String userId, String execId, String storageName, LMST.StorageConfig storageConfig,
                @Nullable TransactionHandle transaction) throws SQLException;

    boolean exists(String userId, String execId) throws SQLException;

    void setKafkaTopicDesc(String execId, KafkaTopicDesc topicDesc, @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateAllocatorSession(String execId, String allocSessionId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void updateAllocateOperationData(String execId, String allocVmOpId, String vmId,
                                     @Nullable TransactionHandle transaction) throws SQLException;

    void updatePortalAddresses(String execId, String apiAddress, String fsAddress,
                               @Nullable TransactionHandle transaction)
        throws SQLException;

    void updatePortalId(String execId, String portalId, @Nullable TransactionHandle transaction) throws SQLException;

    void updatePortalSubjectId(String execId, String subjectId, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setFinishStatus(String execId, Status status, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    KafkaTopicDesc getKafkaTopicDesc(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    LMST.StorageConfig getStorageConfig(String execId) throws SQLException;

    PortalDescription getPortalDescription(String execId) throws SQLException;

    @Nullable
    default String getPortalVmAddress(String execId) throws SQLException {
        var desc = getPortalDescription(execId);
        if (desc != null) {
            return desc.vmAddress().toString();
        }
        return null;
    }

    @Nullable
    String getExpiredExecution() throws SQLException;

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    record KafkaTopicDesc(
        String username,
        String password,  // TODO: encrypt
        String topicName,
        String sinkJobId
    ) {}

    record PortalDescription(
        String portalId,
        String subjectId,
        String allocatorSessionId,
        String vmId,
        HostAndPort vmAddress,
        HostAndPort fsAddress
    ) {}
}
