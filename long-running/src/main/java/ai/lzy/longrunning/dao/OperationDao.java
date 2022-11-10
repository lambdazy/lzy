package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import javax.annotation.Nullable;

public interface OperationDao {
    void create(Operation operation, @Nullable String idempotencyKey, @Nullable String requestChecksum,
                @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation find(String idempotencyKey, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation get(String id, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation updateMetaAndResponse(String id, byte[] meta, byte[] response, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    Operation updateMeta(String id, byte[] meta, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation updateResponse(String id, byte[] response, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Operation updateError(String id, byte[] error, @Nullable TransactionHandle transaction) throws SQLException;
}
