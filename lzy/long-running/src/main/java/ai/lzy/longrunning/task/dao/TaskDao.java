package ai.lzy.longrunning.task.dao;

import ai.lzy.longrunning.task.Task;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

public interface TaskDao {

    @Nullable
    Task get(long id, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Task update(long id, Task.Update update, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Task updateLease(long id, Duration duration, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Task insert(Task task, @Nullable TransactionHandle tx) throws SQLException;

    void delete(long id, @Nullable TransactionHandle tx) throws SQLException;

    List<Task> lockPendingBatch(String ownerId, Duration leaseTime, int batchSize, @Nullable TransactionHandle tx)
        throws SQLException;
}
