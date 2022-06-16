package ru.yandex.cloud.ml.platform.lzy.graph.db.impl;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import ru.yandex.cloud.ml.platform.lzy.graph.db.DaoException;
import ru.yandex.cloud.ml.platform.lzy.graph.db.QueueEventDao;
import ru.yandex.cloud.ml.platform.lzy.graph.db.Storage;
import ru.yandex.cloud.ml.platform.lzy.graph.db.Utils;
import ru.yandex.cloud.ml.platform.lzy.graph.model.QueueEvent;

@Singleton
public class QueueEventDaoImpl implements QueueEventDao {
    private final Storage storage;

    @Inject
    public QueueEventDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void add(QueueEvent.Type type, String workflowId, String graphId, String description) throws DaoException {
        try (
            Connection con = storage.connect();
            PreparedStatement st = con.prepareStatement(
                """
                INSERT INTO queue_event (
                    id, type, workflow_id, graph_id, acquired, description
                ) VALUES (?, ?, ?, ?, ?, ?)""")) {

            final QueueEvent event = new QueueEvent(
                UUID.randomUUID().toString(), workflowId,
                graphId, type, description
            );
            st.setString(1, event.id());
            st.setString(2, event.type().name());
            st.setString(3, event.workflowId());
            st.setString(4, event.graphId());
            st.setBoolean(5, false);
            st.setString(6, event.description());
            st.execute();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<QueueEvent> acquireWithLimit(int limit) throws DaoException {
        final List<QueueEvent> ret = new ArrayList<>();
        Utils.executeInTransaction(storage, con -> {
            try (PreparedStatement st = con.prepareStatement(
                """
                SELECT id, type, workflow_id, graph_id, description
                 FROM queue_event
                 WHERE acquired = false
                 LIMIT ?
                 FOR UPDATE""")) {
                st.setInt(1, limit);

                try (final ResultSet s = st.executeQuery()) {
                    if (!s.isBeforeFirst()) {
                        return;
                    }

                    while (s.next()) {
                        final QueueEvent event = new QueueEvent(
                            s.getString(1),
                            s.getString(3),
                            s.getString(4),
                            QueueEvent.Type.valueOf(s.getString(2)),
                            s.getString(5)
                        );
                        ret.add(event);
                    }

                }
            }

            try (PreparedStatement st = con.prepareStatement(
                "UPDATE queue_event SET acquired = true WHERE id = ?")) {
                for (QueueEvent event: ret) {
                    st.setString(1, event.id());
                    st.addBatch();
                }
                st.executeBatch();
            }
        });
        return ret;
    }

    @Override
    public void remove(QueueEvent event) throws DaoException {
        try (
            Connection con = storage.connect();
            PreparedStatement st = con.prepareStatement(
                """
                DELETE FROM queue_event WHERE id = ?""")) {
            if (event.id() == null) {
                throw new DaoException("Cannot delete event with id null");
            }
            st.setString(1, event.id());
            st.execute();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public void removeAllAcquired() throws DaoException {
        try (
            Connection con = storage.connect();
            PreparedStatement st = con.prepareStatement(
                """
                DELETE FROM queue_event WHERE acquired = true""")) {
            st.execute();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }
}
