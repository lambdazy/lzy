package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.Operation;
import com.google.protobuf.Any;

import javax.annotation.Nullable;

public interface OperationDao {
    Operation create(String description, String createdBy, Any meta);

    @Nullable
    Operation get(String opId);

    void update(Operation op);
}
