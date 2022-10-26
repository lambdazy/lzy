package ai.lzy.scheduler.db.impl;

import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.db.ServantEventDao;
import ai.lzy.scheduler.models.ServantEvent;
import com.google.common.net.HostAndPort;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Singleton
public class ServantEventDaoImpl implements ServantEventDao {
    private final SchedulerDataSource storage;

    private static final String FIELDS = " id, \"time\", servant_id, workflow_name, \"type\","
        + " description, rc, task_id, servant_url ";

    public ServantEventDaoImpl(SchedulerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void save(ServantEvent event) {
        try (var con = storage.connect(); var ps = con.prepareStatement(
            "INSERT INTO servant_event ("
                + FIELDS
                + ") VALUES (?, ?, ?, ?, CAST(? AS servant_event_type), ?, ?, ?, ?)")) {
            int count = 0;
            ps.setString(++count, event.id());
            ps.setTimestamp(++count, Timestamp.from(event.timestamp()));
            ps.setString(++count, event.servantId());
            ps.setString(++count, event.workflowName());
            ps.setString(++count, event.type().name());

            ps.setString(++count, event.description());
            ps.setObject(++count, event.rc());
            ps.setString(++count, event.taskId());

            var url = event.servantUrl();
            ps.setString(++count, url == null ? null : url.toString());

            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot save event", e);
        }
    }

    @Nullable
    @Override
    public ServantEvent take(String servantId) throws InterruptedException {
        final ServantEvent[] event = new ServantEvent[1];
        try {
            Transaction.execute(storage, con -> {
                try (var ps = con.prepareStatement(
                    "SELECT" + FIELDS + """
                     FROM servant_event
                     WHERE servant_id = ? AND "time" < current_timestamp
                     ORDER BY "time"
                     LIMIT 1
                     FOR UPDATE""")) {
                    ps.setString(1, servantId);
                    try (var rs = ps.executeQuery()) {
                        if (!rs.isBeforeFirst()) {
                            event[0] = null;
                            return true;
                        }
                        rs.next();
                        event[0] = readEvent(rs);
                    }
                }
                try (var ps = con.prepareStatement("""
                     DELETE FROM servant_event
                     WHERE id = ?""")) {
                    ps.setString(1, event[0].id());
                    ps.execute();
                }

                return true;
            });
        } catch (DaoException e) {
            if (e.getCause() instanceof InterruptedException) {
                throw new InterruptedException();
            }
            throw new RuntimeException("Cannot take event", e);
        }
        return event[0];
    }

    @Override
    public List<ServantEvent> list(String servantId) {
        try (var con = storage.connect(); var ps = con.prepareStatement(
                "SELECT " + FIELDS + "FROM servant_event WHERE servant_id = ?")) {
            ps.setString(1, servantId);

            List<ServantEvent> events = new ArrayList<>();

            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final ServantEvent event = readEvent(rs);
                    events.add(event);
                }
            }
            return events;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot save event", e);
        }
    }

    @NotNull
    private ServantEvent readEvent(ResultSet rs) throws SQLException {
        int count = 0;
        final var id = rs.getString(++count);
        final var timestamp = rs.getTimestamp(++count);
        final var servant = rs.getString(++count);
        final var workflowId = rs.getString(++count);
        final var type = ServantEvent.Type.valueOf(rs.getString(++count));
        final var description = rs.getString(++count);
        final var rc = rs.getObject(++count, Integer.class);
        final var taskId = rs.getString(++count);
        final var servantUrl = rs.getString(++count);
        return new ServantEvent(id, timestamp.toInstant(), servant, workflowId, type, description,
            rc, taskId, servantUrl == null ? null : HostAndPort.fromString(servantUrl));
    }

    @Override
    public void removeAllByTypes(String servantId, ServantEvent.Type... types) {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                DELETE FROM servant_event
                 WHERE servant_id = ? AND "type" = ANY(?)""")) {
            ps.setString(1, servantId);
            var array = con.createArrayOf("servant_event_type", Arrays.stream(types).map(Enum::name).toArray());
            ps.setArray(2, array);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot remove events", e);
        }
    }

    @Override
    public void removeAll(String servantId) {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                DELETE FROM servant_event
                 WHERE servant_id = ?""")) {
            ps.setString(1, servantId);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot remove events", e);
        }
    }
}
