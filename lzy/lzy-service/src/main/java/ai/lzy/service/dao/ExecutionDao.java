package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LMST;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.grpc.Status;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface ExecutionDao {
    void create(String userId, String execId, String storageName, LMST.StorageConfig storageConfig,
                @Nullable TransactionHandle transaction) throws SQLException;

    boolean exists(String userId, String execId) throws SQLException;

    void setKafkaTopicDesc(String execId, @Nullable KafkaTopicDesc topicDesc, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setFinishStatus(String execId, Status status, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    KafkaTopicDesc getKafkaTopicDesc(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    LMST.StorageConfig getStorageConfig(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    StopExecutionState loadStopExecState(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    ExecuteGraphData loadExecGraphData(String execId, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    String getExpiredExecution() throws SQLException;

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    record KafkaTopicDesc(
        String username,
        String password,  // TODO: encrypt
        String topicName,
        @Nullable String sinkJobId
    ) {}

    record ExecuteGraphData(
        LMST.StorageConfig storageConfig,
        ExecutionDao.KafkaTopicDesc kafkaTopicDesc
    ) {}
}
