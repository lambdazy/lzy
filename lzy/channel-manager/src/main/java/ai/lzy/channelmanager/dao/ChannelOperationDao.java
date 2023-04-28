package ai.lzy.channelmanager.dao;

import ai.lzy.channelmanager.operation.ChannelOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface ChannelOperationDao {

    void create(ChannelOperation operation, @Nullable TransactionHandle tx) throws SQLException;

    void update(String operationId, String stateJson, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String operationId, @Nullable TransactionHandle tx) throws SQLException;

    void fail(String operationId, String reason, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    ChannelOperation get(String operationId, @Nullable TransactionHandle tx) throws SQLException;

    List<ChannelOperation> getActiveOperations(@Nullable TransactionHandle tx) throws SQLException;

}
