package ai.lzy.channelmanager.v2.operation;

import ai.lzy.model.db.TransactionHandle;

import javax.annotation.Nullable;

public interface ChannelOperationDao {

    void create(ChannelOperation operation, @Nullable TransactionHandle transaction);

    void update(String operationId, String stateJson, @Nullable TransactionHandle transaction);

    void delete(String operationId, @Nullable TransactionHandle transaction);

}
