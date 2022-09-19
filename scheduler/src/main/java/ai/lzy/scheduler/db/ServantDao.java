package ai.lzy.scheduler.db;

import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.servant.Servant;

import java.util.List;
import javax.annotation.Nullable;

public interface ServantDao {

    @Nullable
    ServantState acquire(String workflowName, String servantId) throws AcquireException, DaoException;
    void updateAndFree(ServantState resource) throws DaoException;

    List<Servant> getAllFree() throws DaoException;
    List<Servant> getAllAcquired() throws DaoException;

    Servant create(String workflowName, Operation.Requirements requirements) throws DaoException;

    @Nullable
    Servant get(String workflowName, String servantId) throws DaoException;
    List<Servant> get(String workflowName) throws DaoException;

    void acquireForTask(String workflowName, String servantId) throws DaoException, AcquireException;
    void freeFromTask(String workflowName, String servantId) throws DaoException;

    void invalidate(Servant servant, String description) throws DaoException;

    class AcquireException extends Exception {}
}
