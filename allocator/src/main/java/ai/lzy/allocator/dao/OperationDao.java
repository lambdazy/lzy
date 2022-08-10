package ai.lzy.allocator.dao;

import ai.lzy.model.db.TransactionManager.TransactionHandle;
import ai.lzy.allocator.model.Operation;
import com.google.protobuf.Any;

import javax.annotation.Nullable;

public interface OperationDao {
    Operation create(String description, String createdBy, Any meta, @Nullable TransactionHandle transaction);

    @Nullable
    Operation get(String opId, @Nullable TransactionHandle transaction);

    void update(Operation op, @Nullable TransactionHandle transaction);
}
