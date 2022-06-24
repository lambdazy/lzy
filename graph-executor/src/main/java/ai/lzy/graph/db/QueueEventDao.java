package ai.lzy.graph.db;

import ai.lzy.graph.model.QueueEvent;
import java.util.List;

public interface QueueEventDao {

    void add(QueueEvent.Type type, String workflowId, String graphId, String description) throws DaoException;

    List<QueueEvent> acquireWithLimit(int limit) throws DaoException;

    void remove(QueueEvent event) throws DaoException;

    void removeAllAcquired() throws DaoException;
}
