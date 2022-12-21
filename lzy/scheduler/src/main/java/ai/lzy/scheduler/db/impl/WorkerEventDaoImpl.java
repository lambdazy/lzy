package ai.lzy.scheduler.db.impl;

import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.db.WorkerEventDao;
import ai.lzy.scheduler.models.WorkerEvent;
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
public class WorkerEventDaoImpl implements WorkerEventDao {
    private final SchedulerDataSource storage;

    private static final String FIELDS = " id, \"time\", worker_id, workflow_name, \"type\","
        + " description, rc, task_id, worker_url ";

    public WorkerEventDaoImpl(SchedulerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void save(WorkerEvent event) {
        try (var con = storage.connect(); var ps = con.prepareStatement(
            "INSERT INTO worker_event ("
                + FIELDS
                + ") VALUES (?, ?, ?, ?, CAST(? AS worker_event_type), ?, ?, ?, ?)"))
        {
            int count = 0;
            ps.setString(++count, event.id());
            ps.setTimestamp(++count, Timestamp.from(event.timestamp()));
            ps.setString(++count, event.workerId());
            ps.setString(++count, event.workflowName());
            ps.setString(++count, event.type().name());

            ps.setString(++count, event.description());
            ps.setObject(++count, event.rc());
            ps.setString(++count, event.taskId());

            var url = event.workerUrl();
            ps.setString(++count, url == null ? null : url.toString());

            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot save event", e);
        }
    }

    @Nullable
    @Override
    public WorkerEvent take(String workerId) throws InterruptedException {
        final WorkerEvent[] event = new WorkerEvent[1];
        try {
            Transaction.execute(storage, con -> {
                try (var ps = con.prepareStatement(
                    "SELECT" + FIELDS + """
                     FROM worker_event
                     WHERE worker_id = ? AND "time" < current_timestamp
                     ORDER BY "time"
                     LIMIT 1
                     FOR UPDATE"""))
                {
                    ps.setString(1, workerId);
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
                     DELETE FROM worker_event
                     WHERE id = ?"""))
                {
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
    public List<WorkerEvent> list(String workerId) {
        try (var con = storage.connect(); var ps = con.prepareStatement(
                "SELECT " + FIELDS + "FROM worker_event WHERE worker_id = ?"))
        {
            ps.setString(1, workerId);

            List<WorkerEvent> events = new ArrayList<>();

            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final WorkerEvent event = readEvent(rs);
                    events.add(event);
                }
            }
            return events;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot save event", e);
        }
    }

    @NotNull
    private WorkerEvent readEvent(ResultSet rs) throws SQLException {
        int count = 0;
        final var id = rs.getString(++count);
        final var timestamp = rs.getTimestamp(++count);
        final var worker = rs.getString(++count);
        final var workflowId = rs.getString(++count);
        final var type = WorkerEvent.Type.valueOf(rs.getString(++count));
        final var description = rs.getString(++count);
        final var rc = rs.getObject(++count, Integer.class);
        final var taskId = rs.getString(++count);
        final var workerUrl = rs.getString(++count);
        return new WorkerEvent(id, timestamp.toInstant(), worker, workflowId, type, description,
            rc, taskId, workerUrl == null ? null : HostAndPort.fromString(workerUrl));
    }

    @Override
    public void removeAllByTypes(String workerId, WorkerEvent.Type... types) {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                DELETE FROM worker_event
                 WHERE worker_id = ? AND "type" = ANY(?)"""))
        {
            ps.setString(1, workerId);
            var array = con.createArrayOf("worker_event_type", Arrays.stream(types).map(Enum::name).toArray());
            ps.setArray(2, array);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot remove events", e);
        }
    }

    @Override
    public void removeAll(String workerId) {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                DELETE FROM worker_event
                 WHERE worker_id = ?"""))
        {
            ps.setString(1, workerId);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Cannot remove events", e);
        }
    }
}
