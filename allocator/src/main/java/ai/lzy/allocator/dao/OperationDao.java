package ai.lzy.allocator.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;

import java.sql.SQLException;
import javax.annotation.Nullable;

public interface OperationDao {
    Operation create(String description, String createdBy, Any meta, @Nullable TransactionHandle th)
        throws SQLException;

    @Nullable
    Operation get(String opId, @Nullable TransactionHandle transaction) throws SQLException;

    void update(Operation op, @Nullable TransactionHandle transaction) throws SQLException;
}
