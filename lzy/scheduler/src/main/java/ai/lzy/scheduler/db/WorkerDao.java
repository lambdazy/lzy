package ai.lzy.scheduler.db;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.models.WorkerState;
import ai.lzy.scheduler.worker.Worker;

import java.util.List;
import javax.annotation.Nullable;

public interface WorkerDao {

    @Nullable
    WorkerState acquire(String workflowName, String workerId) throws AcquireException, DaoException;
    void updateAndFree(WorkerState resource) throws DaoException;

    List<Worker> getAllFree() throws DaoException;
    List<Worker> getAllAcquired() throws DaoException;

    Worker create(String userId, String workflowName, Operation.Requirements requirements,
                  @Nullable TransactionHandle tx) throws DaoException;

    default Worker create(String userId, String workflowName, Operation.Requirements requirements) throws DaoException {
        return create(userId, workflowName, requirements, null);
    }

    @Nullable
    Worker get(String workflowName, String workerId) throws DaoException;
    List<Worker> get(String workflowName) throws DaoException;

    void acquireForTask(String workflowName, String workerId,
                        @Nullable TransactionHandle tx) throws DaoException, AcquireException;
    default void acquireForTask(String workflowName, String workerId) throws DaoException, AcquireException {
        acquireForTask(workflowName, workerId, null);
    }
    void freeFromTask(String workflowName, String workerId) throws DaoException;

    void invalidate(Worker worker, String description) throws DaoException;

    class AcquireException extends Exception {}
}
