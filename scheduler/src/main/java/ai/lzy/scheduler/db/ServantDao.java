package ai.lzy.scheduler.db;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.servant.Servant;
import java.util.List;
import javax.annotation.Nullable;

public interface ServantDao {

    @Nullable
    ServantState acquire(String workflowId, String servantId) throws AcquireException, DaoException;
    void updateAndFree(ServantState resource) throws DaoException;

    List<Servant> getAllFree() throws DaoException;
    List<Servant> getAllAcquired() throws DaoException;

    Servant create(String workflowId, Provisioning provisioning) throws DaoException;
    int countAlive(String workflowId, Provisioning provisioning) throws DaoException;

    @Nullable
    Servant get(String workflowId, String servantId) throws DaoException;

    @Nullable
    Servant acquireForTask(String workflowId, Provisioning provisioning, Status... statuses) throws DaoException;
    void freeFromTask(String workflowId, String servantId) throws DaoException;

    void invalidate(Servant servant, String description) throws DaoException;
    List<Servant> get(String workflowId) throws DaoException;

    class AcquireException extends Exception {}
}
