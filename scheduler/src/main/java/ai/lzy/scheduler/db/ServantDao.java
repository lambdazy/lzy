package ai.lzy.scheduler.db;

import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.ServantState.Status;
import ai.lzy.scheduler.servant.Servant;
import java.util.List;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;

public interface ServantDao {

    @Nullable
    ServantState acquire(String resourceId) throws AcquireException;
    void updateAndFree(ServantState resource);

    List<Servant> getAllFree();
    List<Servant> getAllAcquired();

    Servant create(String workflowId, Provisioning provisioning, Env env) throws DaoException;

    @Nullable
    Servant get(String workflowId, String servantId);

    @Nullable
    Servant acquireForTask(String workflowId, String taskId,
                           Provisioning provisioning, Status... statuses) throws DaoException;
    void acquireForTask(Servant servant, String taskId) throws DaoException;
    void freeFromTask(String servantId) throws DaoException;

    class AcquireException extends Exception {}
}
