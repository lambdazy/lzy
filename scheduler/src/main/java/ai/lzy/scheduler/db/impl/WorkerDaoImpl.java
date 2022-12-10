package ai.lzy.scheduler.db.impl;

import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.model.operation.Operation;
import ai.lzy.scheduler.allocator.WorkerMetaStorage;
import ai.lzy.scheduler.db.WorkerDao;
import ai.lzy.scheduler.models.WorkerState;
import ai.lzy.scheduler.models.WorkerState.WorkerStateBuilder;
import ai.lzy.scheduler.worker.Worker;
import ai.lzy.scheduler.worker.impl.EventQueueManager;
import ai.lzy.scheduler.worker.impl.WorkerImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class WorkerDaoImpl implements WorkerDao, WorkerMetaStorage {

    private final SchedulerDataSource storage;
    private final EventQueueManager queue;

    private static final String FIELDS = "id, user_id, workflow_name, status, requirements_json,"
            + " error_description, task_id, worker_url";

    @Inject
    public WorkerDaoImpl(SchedulerDataSource storage, EventQueueManager queue) {
        this.storage = storage;
        this.queue = queue;
    }

    @Nullable
    @Override
    public WorkerState acquire(String workflowName, String workerId) throws AcquireException, DaoException {
        final int affected;
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                UPDATE worker SET acquired = true
                 WHERE id = ? AND workflow_name = ? AND acquired = false""")) {
            ps.setString(1, workerId);
            ps.setString(2, workflowName);
            affected = ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
        final var state = getState(workflowName, workerId);
        if (state == null) {
            return null;
        }
        if (affected == 0) {
            throw new AcquireException();
        }
        return state;
    }

    @Override
    public void updateAndFree(WorkerState resource) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " UPDATE worker SET (" + FIELDS + ", acquired) = (?, ?, ?, CAST(? AS worker_status), ?, ?, ?, ?, false) "
                + " WHERE workflow_name = ? AND  id = ?")) {
            writeState(resource, con, ps);
            ps.setString(9, resource.workflowName());
            ps.setString(10, resource.id());
            ps.executeUpdate();
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<Worker> getAllFree() throws DaoException {
        final List<WorkerState> states;
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " SELECT " + FIELDS + " FROM worker"
                + " WHERE acquired = false AND status != 'DESTROYED'")) {
            states = readWorkerStates(ps);
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
        return states.stream().map(s -> (Worker) new WorkerImpl(s, queue.get(s.workflowName(), s.id()))).toList();
    }

    @Override
    public List<Worker> getAllAcquired() throws DaoException {
        final List<WorkerState> states;
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " SELECT " + FIELDS + " FROM worker"
                + " WHERE acquired = true AND status != 'DESTROYED'")) {
            states = readWorkerStates(ps);
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
        return states.stream().map(s -> (Worker) new WorkerImpl(s, queue.get(s.workflowName(), s.id()))).toList();
    }

    @Override
    public Worker create(String userId, String workflowName, Operation.Requirements requirements) throws DaoException {
        final var id = UUID.randomUUID().toString();
        final var status = WorkerState.Status.CREATED;
        final var state = new WorkerStateBuilder(id, userId, workflowName, requirements, status).build();
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " INSERT INTO worker(" + FIELDS + ")"
                + " VALUES (?, ?, ?, CAST(? AS worker_status), ?, ?, ?, ?)")) {
            writeState(state, con, ps);
            ps.execute();
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
        return new WorkerImpl(state, queue.get(state.workflowName(), state.id()));
    }

    @Nullable
    @Override
    public Worker get(String workflowName, String workerId) throws DaoException {
        final var state = getState(workflowName, workerId);
        if (state == null) {
            return null;
        }
        return new WorkerImpl(state, queue.get(state.workflowName(), state.id()));
    }

    @Override
    public List<Worker> get(String workflowName) throws DaoException {
        final List<WorkerState> states;
        try (var con = storage.connect(); var ps = con.prepareStatement(
                " SELECT " + FIELDS + " FROM worker"
                        + " WHERE workflow_name = ? AND status != 'DESTROYED'")) {
            ps.setString(1, workflowName);
            states = readWorkerStates(ps);
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
        return states.stream().map(s -> (Worker) new WorkerImpl(s, queue.get(s.workflowName(), s.id()))).toList();
    }

    @Override
    public void acquireForTask(String workflowName, String workerId) throws DaoException, AcquireException {
        AtomicBoolean failed = new AtomicBoolean(false);
        Transaction.execute(storage, con -> {
            final WorkerState state;
            try (var ps = con.prepareStatement(
                "SELECT " + FIELDS + " FROM worker " + """
                WHERE workflow_name = ? AND id = ? AND acquired_for_task = false
                FOR UPDATE
                """)) {
                ps.setString(1, workflowName);
                ps.setString(2, workerId);
                try (var rs = ps.executeQuery()) {
                    if (!rs.isBeforeFirst()) {
                        failed.set(true);
                        return true;
                    }
                    rs.next();
                    state = readWorkerState(rs);
                }
            }
            try (var ps = con.prepareStatement("""
                UPDATE worker
                SET acquired_for_task = true
                WHERE workflow_name = ? AND id = ?
                """)) {
                ps.setString(1, state.workflowName());
                ps.setString(2, state.id());
                ps.executeUpdate();
            }

            return true;
        });
        if (failed.get()) {
            throw new AcquireException();
        }
    }

    @Override
    public void freeFromTask(String workflowName, String workerId) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                UPDATE worker
                SET acquired_for_task = false
                WHERE workflow_name = ? AND id = ?""")) {
            ps.setString(1, workflowName);
            ps.setString(2, workerId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public void invalidate(Worker worker, String description) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                UPDATE worker
                SET (acquired_for_task, acquired, status, error_description) = (false, false, 'DESTROYED', ?)
                WHERE workflow_name = ? AND id = ?""")) {
            ps.setString(1, description);
            ps.setString(2, worker.workflowName());
            ps.setString(3, worker.id());
            int updated = ps.executeUpdate();
            if (updated < 1) {
                throw new DaoException("Not updated");
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    private void writeState(WorkerState state, Connection con, PreparedStatement ps)
            throws SQLException, JsonProcessingException {
        final var mapper = new ObjectMapper();

        int paramCount = 0;
        var url = state.workerUrl();
        ps.setString(++paramCount, state.id());
        ps.setString(++paramCount, state.userId());
        ps.setString(++paramCount, state.workflowName());
        ps.setString(++paramCount, state.status().name());
        ps.setString(++paramCount, mapper.writeValueAsString(state.requirements()));
        ps.setString(++paramCount, state.errorDescription());
        ps.setString(++paramCount, state.taskId());
        ps.setString(++paramCount, url == null ? null : url.toString());
    }

    private WorkerState readWorkerState(ResultSet rs) throws SQLException, JsonProcessingException {
        final var mapper = new ObjectMapper();

        int resCount = 0;
        final var id = rs.getString(++resCount);
        final var userId = rs.getString(++resCount);
        final var workflowName = rs.getString(++resCount);
        final var status = WorkerState.Status.valueOf(rs.getString(++resCount));
        final var requirements = mapper.readValue(rs.getString(++resCount), Operation.Requirements.class);
        final var errorDescription = rs.getString(++resCount);
        final var taskId = rs.getString(++resCount);
        final var workerUrl = rs.getString(++resCount);
        return new WorkerState(id, userId, workflowName, requirements,
            status, errorDescription, taskId, workerUrl == null ? null : HostAndPort.fromString(workerUrl));
    }

    @Nullable
    private WorkerState getState(String workflowId, String workerId) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
                " SELECT " + FIELDS + " FROM worker"
                    + " WHERE id = ? AND workflow_name = ?")) {
            ps.setString(1, workerId);
            ps.setString(2, workflowId);
            try (var rs = ps.executeQuery()) {
                if (!rs.isBeforeFirst()) {
                    return null;
                }
                rs.next();
                return readWorkerState(rs);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    private List<WorkerState> readWorkerStates(PreparedStatement ps) throws SQLException, JsonProcessingException {
        final List<WorkerState> res = new ArrayList<>();
        try (var rs = ps.executeQuery()) {
            while (rs.next()) {
                var state = readWorkerState(rs);
                res.add(state);
            }
        }
        return res;
    }

    @Override
    public void clear(String workflowName, String workerId) {
        try (var con = storage.connect(); var ps = con.prepareStatement(""" 
             UPDATE worker
             SET allocator_meta = null
             WHERE id = ? AND workflow_name = ?""")) {
            ps.setString(1, workerId);
            ps.setString(2, workflowName);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveMeta(String workflowName, String workerId, String meta) {
        try (var con = storage.connect(); var ps = con.prepareStatement(""" 
             UPDATE worker
             SET allocator_meta = ?
             WHERE id = ? AND workflow_name = ?""")) {
            ps.setString(1, meta);
            ps.setString(2, workerId);
            ps.setString(3, workflowName);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    @Override
    public String getMeta(String workflowName, String workerId) {
        try (var con = storage.connect(); var ps = con.prepareStatement(""" 
             SELECT allocator_meta FROM worker
             WHERE id = ? AND workflow_name = ?""")) {
            ps.setString(1, workerId);
            ps.setString(2, workflowName);
            try (var rs = ps.executeQuery()) {
                if (!rs.isBeforeFirst()) {
                    return null;
                }
                rs.next();
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
