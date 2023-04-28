package ai.lzy.graph.db;

import ai.lzy.graph.model.QueueEvent;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface QueueEventDao {

    void add(QueueEvent.Type type, String workflowId, String graphId, String description,
             @Nullable TransactionHandle transaction) throws SQLException;

    List<QueueEvent> acquireWithLimit(int limit) throws DaoException;

    void remove(QueueEvent event) throws DaoException;

    void removeAllAcquired() throws DaoException;
}
