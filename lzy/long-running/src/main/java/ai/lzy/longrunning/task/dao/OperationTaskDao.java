package ai.lzy.longrunning.task.dao;

import ai.lzy.longrunning.task.OperationTask;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

public interface OperationTaskDao {

    @Nullable
    OperationTask get(long id, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    OperationTask update(long id, OperationTask.Update update, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    OperationTask updateLease(long id, Duration duration, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    OperationTask insert(OperationTask operationTask, @Nullable TransactionHandle tx) throws SQLException;

    void delete(long id, @Nullable TransactionHandle tx) throws SQLException;

    List<OperationTask> lockPendingBatch(String ownerId, Duration leaseDuration, int batchSize,
                                         @Nullable TransactionHandle tx) throws SQLException;

    List<OperationTask> recaptureOldTasks(String ownerId, Duration leaseDuration, @Nullable TransactionHandle tx)
        throws SQLException;

    @Nullable
    OperationTask tryLockTask(Long taskId, String entityId, String ownerId, Duration leaseDuration,
                              @Nullable TransactionHandle tx) throws SQLException;

    List<OperationTask> getAll(@Nullable TransactionHandle tx) throws SQLException;
}
