package ai.lzy.channelmanager.v2.dao;

import ai.lzy.channelmanager.v2.operation.ChannelOperation;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

public interface ChannelOperationDao {

    void create(ChannelOperation operation, @Nullable TransactionHandle tx) throws SQLException;

    void update(String operationId, String stateJson, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String operationId, @Nullable TransactionHandle tx) throws SQLException;

    void fail(String operationId, String reason, @Nullable TransactionHandle tx) throws SQLException;

    ChannelOperation get(String operationId, @Nullable TransactionHandle tx) throws SQLException;

    List<ChannelOperation> getActiveOperations(@Nullable TransactionHandle tx) throws SQLException;

}
