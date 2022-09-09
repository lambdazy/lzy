package ai.lzy.allocator.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.allocator.model.Operation;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.Any;

import javax.annotation.Nullable;
import java.sql.SQLException;

public interface OperationDao {
    Operation create(String description, String createdBy, Any meta, @Nullable TransactionHandle th)
        throws SQLException;

    @Nullable
    Operation get(String opId, @Nullable TransactionHandle transaction) throws SQLException;

    void update(Operation op, @Nullable TransactionHandle transaction) throws SQLException;
}
