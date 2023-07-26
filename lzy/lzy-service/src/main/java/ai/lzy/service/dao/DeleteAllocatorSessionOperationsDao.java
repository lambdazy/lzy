package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface DeleteAllocatorSessionOperationsDao {

    void create(String opId, String sessionId, String instanceId, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String opId, @Nullable TransactionHandle tx) throws SQLException;

    void setAllocatorOperationId(String opId, String allocOpId, @Nullable TransactionHandle tx) throws SQLException;

    List<OpState> list(String instanceId, @Nullable TransactionHandle tx) throws SQLException;

    record OpState(
        String opId,
        String opDesc,
        String idempotencyKey,
        String sessionId,
        @Nullable String allocOpId
    ) {}
}
