package ai.lzy.scheduler.db.impl;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.db.Storage;
import ai.lzy.scheduler.db.Utils;
import ai.lzy.scheduler.models.ServantState;
import ai.lzy.scheduler.models.ServantState.ServantStateBuilder;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.servant.impl.EventQueueManager;
import ai.lzy.scheduler.servant.impl.ServantImpl;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
public class ServantDaoImpl implements ServantDao {

    private final Storage storage;
    private final EventQueueManager queue;

    private static final String FIELDS = "id, workflow_id, status, provisioning,"
            + " error_description, task_id, servant_url";

    @Inject
    public ServantDaoImpl(Storage storage, EventQueueManager queue) {
        this.storage = storage;
        this.queue = queue;
    }

    @Nullable
    @Override
    public ServantState acquire(String workflowId, String servantId) throws AcquireException, DaoException {
        final int affected;
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                UPDATE servant SET acquired = true
                 WHERE id = ? AND workflow_id = ? AND acquired = false""")) {
            ps.setString(1, servantId);
            ps.setString(2, workflowId);
            affected = ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
        final var state = getState(workflowId, servantId);
        if (state == null) {
            return null;
        }
        if (affected == 0) {
            throw new AcquireException();
        }
        return state;
    }

    @Override
    public void updateAndFree(ServantState resource) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " UPDATE servant SET (" + FIELDS + ", acquired) = (?, ?, ?, ?, ?, ?, ?, false) "
                + " WHERE workflow_id = ? AND  id = ?")) {
            writeState(resource, con, ps);
            ps.setString(8, resource.workflowId());
            ps.setString(9, resource.id());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<Servant> getAllFree() throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " SELECT " + FIELDS + " FROM servant"
                + " WHERE acquired = false AND status != 'DESTROYED'")) {
            return readServants(ps);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<Servant> getAllAcquired() throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " SELECT " + FIELDS + " FROM servant"
                + " WHERE acquired = true AND status != 'DESTROYED'")) {
            return readServants(ps);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public Servant create(String workflowId, Provisioning provisioning) throws DaoException {
        final var id = UUID.randomUUID().toString();
        final var status = ServantState.Status.CREATED;
        final var state = new ServantStateBuilder(id, workflowId, provisioning, status).build();
        try (var con = storage.connect(); var ps = con.prepareStatement(
            " INSERT INTO servant(" + FIELDS + ")"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            writeState(state, con, ps);
            ps.execute();
            return new ServantImpl(state, queue.get(state.workflowId(), state.id()));
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public int countAlive(String workflowId, Provisioning provisioning) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                SELECT count() FROM servant
                WHERE status != 'DESTROYED' AND provisioning @> ?""")) {
            var array = con.createArrayOf("varchar", provisioning.tags().toArray());
            ps.setArray(1, array);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Nullable
    @Override
    public Servant get(String workflowId, String servantId) throws DaoException {
        final var state = getState(workflowId, servantId);
        if (state == null) {
            return  null;
        }
        return new ServantImpl(state, queue.get(state.workflowId(), state.id()));
    }

    @Override
    public List<Servant> get(String workflowId) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
                " SELECT " + FIELDS + " FROM servant"
                        + " WHERE workflow_id = ? AND status != 'DESTROYED'")) {
            ps.setString(1, workflowId);
            return readServants(ps);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public void acquireForTask(String workflowId, String servantId) throws DaoException, AcquireException {
        AtomicBoolean failed = new AtomicBoolean(false);
        Utils.executeInTransaction(storage, con -> {
            final ServantState state;
            try (var ps = con.prepareStatement(
                "SELECT " + FIELDS + " FROM servant " + """
                WHERE workflow_id = ? AND id = ? AND acquired_for_task = false
                FOR UPDATE
                """)) {
                ps.setString(1, workflowId);
                ps.setString(2, servantId);
                try (var rs = ps.executeQuery()) {
                    if (!rs.isBeforeFirst()) {
                        failed.set(true);
                        return;
                    }
                    rs.next();
                    state = readServantState(rs);
                }
            }
            try (var ps = con.prepareStatement("""
                UPDATE servant
                SET acquired_for_task = true
                WHERE workflow_id = ? AND id = ?
                """)) {
                ps.setString(1, state.workflowId());
                ps.setString(2, state.id());
                ps.executeUpdate();
            }
        });
        if (failed.get()) {
            throw new AcquireException();
        }
    }

    @Override
    public void freeFromTask(String workflowId, String servantId) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                UPDATE servant
                SET acquired_for_task = false
                WHERE workflow_id = ? AND id = ?""")) {
            ps.setString(1, workflowId);
            ps.setString(2, servantId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public void invalidate(Servant servant, String description) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement("""
                UPDATE servant
                SET (acquired_for_task, acquired, status, error_description) = (false, false, 'DESTROYED', ?)
                WHERE workflow_id = ? AND id = ?""")) {
            ps.setString(1, description);
            ps.setString(2, servant.workflowId());
            ps.setString(3, servant.id());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    private void writeState(ServantState state, Connection con, PreparedStatement ps) throws SQLException {
        int paramCount = 0;
        var url = state.servantUrl();
        ps.setString(++paramCount, state.id());
        ps.setString(++paramCount, state.workflowId());
        ps.setString(++paramCount, state.status().name());
        ps.setArray(++paramCount, con.createArrayOf("varchar", state.provisioning().tags().toArray()));
        ps.setString(++paramCount, state.errorDescription());
        ps.setString(++paramCount, state.taskId());
        ps.setString(++paramCount, url == null ? null : url.toString());
    }

    private ServantState readServantState(ResultSet rs) throws SQLException {
        int resCount = 0;
        final var id = rs.getString(++resCount);
        final var workflow = rs.getString(++resCount);
        final var status = ServantState.Status.valueOf(rs.getString(++resCount));
        final var arr = (Object[]) rs.getArray(++resCount).getArray();
        final var provisioning = Arrays.copyOf(arr, arr.length, String[].class);
        final var errorDescription = rs.getString(++resCount);
        final var taskId = rs.getString(++resCount);
        final var servantUrl = rs.getString(++resCount);
        return new ServantState(id, workflow, () -> Arrays.stream(provisioning).collect(Collectors.toSet()),
                status, errorDescription, taskId, servantUrl == null ? null : HostAndPort.fromString(servantUrl));
    }

    @Nullable
    private ServantState getState(String workflowId, String servantId) throws DaoException {
        try (var con = storage.connect(); var ps = con.prepareStatement(
                " SELECT " + FIELDS + " FROM servant"
                    + " WHERE id = ? AND workflow_id = ?")) {
            ps.setString(1, servantId);
            ps.setString(2, workflowId);
            try (var rs = ps.executeQuery()) {
                if (!rs.isBeforeFirst()) {
                    return null;
                }
                rs.next();
                return readServantState(rs);
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    private List<Servant> readServants(PreparedStatement ps) throws SQLException {
        final List<Servant> res = new ArrayList<>();
        try (var rs = ps.executeQuery()) {
            while (rs.next()) {
                var state = readServantState(rs);
                res.add(new ServantImpl(state, queue.get(state.workflowId(), state.id())));
            }
        }
        return res;
    }
}
