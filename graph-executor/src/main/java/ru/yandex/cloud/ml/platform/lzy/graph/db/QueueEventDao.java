package ru.yandex.cloud.ml.platform.lzy.graph.db;

import java.util.List;
import javax.annotation.Nullable;
import ru.yandex.cloud.ml.platform.lzy.graph.model.QueueEvent;

public interface QueueEventDao {

    void add(QueueEvent.Type type, String workflowId, String graphId, String description) throws DaoException;

    List<QueueEvent> acquireWithLimit(int limit) throws DaoException;

    void remove(QueueEvent event) throws DaoException;

    void removeAllAcquired() throws DaoException;
}
