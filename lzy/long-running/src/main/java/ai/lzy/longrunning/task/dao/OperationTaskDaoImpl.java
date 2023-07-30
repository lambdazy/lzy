package ai.lzy.longrunning.task.dao;

import ai.lzy.longrunning.task.OperationTask;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OperationTaskDaoImpl implements OperationTaskDao {

    private static final String FIELDS = "id, name, entity_id, type, status, created_at, updated_at, metadata," +
        " operation_id, worker_id, lease_till";
    public static final String SELECT_QUERY = "SELECT %s FROM operation_task WHERE id = ?".formatted(FIELDS);
    public static final String INSERT_QUERY = """
        INSERT INTO operation_task (name, entity_id, type, status, created_at, updated_at, metadata, operation_id)
        VALUES (?, ?, cast(? as task_type), cast(? as task_status), now(), now(), cast(? as jsonb), ?)
        RETURNING %s;
        """.formatted(FIELDS);

    //in first nested request we gather all tasks that either locked or free.
    //in second nested request we filter result of previous request to get only free tasks
    //and select only specific amount.
    private static final String LOCK_PENDING_BATCH_QUERY = """
        UPDATE operation_task
        SET status = 'RUNNING', worker_id = ?, updated_at = now(), lease_till = now() + cast(? as interval)
        WHERE id IN (
            SELECT id
            FROM operation_task
            WHERE id IN (
                SELECT DISTINCT ON (entity_id) id
                FROM operation_task
                WHERE status IN ('PENDING', 'RUNNING')
                ORDER BY entity_id, id
            ) AND status = 'PENDING'
            LIMIT ?
        )
        RETURNING %s;
        """.formatted(FIELDS);

    private static final String RECAPTURE_OLD_TASKS_QUERY = """
        UPDATE operation_task
        SET updated_at = now(), lease_till = now() + cast(? as interval)
        WHERE status = 'RUNNING' AND worker_id = ?
        RETURNING %s;
        """.formatted(FIELDS);

    //in first nested request we select all tasks by entity_id that are either locked or free and take the first one.
    //in second nested request we filter result of previous request to get only pending task with specific id.
    //if we get the task then we lock it and return it.
    private static final String TRY_LOCK_TASK_QUERY = """
        UPDATE operation_task
        SET status = 'RUNNING', worker_id = ?, updated_at = now(), lease_till = now() + cast(? as interval)
        WHERE id IN (
            SELECT id
            FROM operation_task
            WHERE id IN (
                SELECT id
                FROM operation_task
                WHERE status IN ('PENDING', 'RUNNING') AND entity_id = ?
                ORDER BY id
                LIMIT 1
            ) AND status = 'PENDING' AND id = ?
        )
        RETURNING %s;
        """.formatted(FIELDS);

    public static final String DELETE_QUERY = "DELETE FROM operation_task WHERE id = ?";
    public static final String UPDATE_LEASE_QUERY = """
        UPDATE operation_task
        SET lease_till = now() + cast(? as interval)
        WHERE id = ?
        RETURNING %s
        """.formatted(FIELDS);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() { };


    private final Storage storage;
    private final ObjectMapper objectMapper;

    public OperationTaskDaoImpl(Storage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Nullable
    @Override
    public OperationTask get(long id, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(SELECT_QUERY)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return readTask(rs);
                    }
                    return null;
                }
            }
        });
    }

    @Override
    @Nullable
    public OperationTask update(long id, OperationTask.Update update, @Nullable TransactionHandle tx)
        throws SQLException
    {
        if (update.isEmpty()) {
            return null;
        }
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(prepareUpdateQuery(update))) {
                prepareUpdateParameters(ps, id, update);
                var rs = ps.executeQuery();
                if (rs.next()) {
                    return readTask(rs);
                }
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    @Nullable
    public OperationTask updateLease(long id, Duration duration, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(UPDATE_LEASE_QUERY)) {
                ps.setString(1, duration.toString());
                ps.setLong(2, id);
                var rs = ps.executeQuery();
                if (rs.next()) {
                    return readTask(rs);
                }
                return null;
            }
        });
    }

    @Nullable
    @Override
    public OperationTask insert(OperationTask operationTask, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(INSERT_QUERY)) {
                var i = 0;
                ps.setString(++i, operationTask.name());
                ps.setString(++i, operationTask.entityId());
                ps.setString(++i, operationTask.type());
                ps.setString(++i, operationTask.status().name());
                ps.setString(++i, objectMapper.writeValueAsString(operationTask.metadata()));
                ps.setString(++i, operationTask.operationId());
                var rs = ps.executeQuery();
                if (rs.next()) {
                    return readTask(rs);
                }
                return null;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void delete(long id, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(DELETE_QUERY)) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
        });
    }

    @Override
    public List<OperationTask> lockPendingBatch(String ownerId, Duration leaseDuration, int batchSize,
                                                @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(LOCK_PENDING_BATCH_QUERY)) {
                var i = 0;
                ps.setString(++i, ownerId);
                ps.setString(++i, leaseDuration.toString());
                ps.setInt(++i, batchSize);
                try (var rs = ps.executeQuery()) {
                    var result = new ArrayList<OperationTask>(rs.getFetchSize());
                    while (rs.next()) {
                        result.add(readTask(rs));
                    }
                    return result;
                }
            }
        });
    }

    @Override
    public List<OperationTask> recaptureOldTasks(String ownerId, Duration leaseTime, @Nullable TransactionHandle tx)
        throws SQLException
    {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(RECAPTURE_OLD_TASKS_QUERY)) {
                var i = 0;
                ps.setString(++i, leaseTime.toString());
                ps.setString(++i, ownerId);
                try (var rs = ps.executeQuery()) {
                    var result = new ArrayList<OperationTask>(rs.getFetchSize());
                    while (rs.next()) {
                        result.add(readTask(rs));
                    }
                    return result;
                }
            }
        });
    }

    @Nullable
    @Override
    public OperationTask tryLockTask(Long taskId, String entityId, String ownerId, Duration leaseDuration,
                                     @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(TRY_LOCK_TASK_QUERY)) {
                var i = 0;
                ps.setString(++i, ownerId);
                ps.setString(++i, leaseDuration.toString());
                ps.setString(++i, entityId);
                ps.setLong(++i, taskId);
                try (var rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return readTask(rs);
                    }
                    return null;
                }
            }
        });
    }

    private static String prepareUpdateQuery(OperationTask.Update update) {
        var sb = new StringBuilder("UPDATE operation_task SET ");
        if (update.status() != null) {
            sb.append("status = cast(? as task_status), ");
        }
        if (update.metadata() != null) {
            sb.append("metadata = cast(? as jsonb), ");
        }
        if (update.operationId() != null) {
            sb.append("operation_id = ?, ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(" WHERE id = ? RETURNING ").append(FIELDS);
        return sb.toString();
    }

    private void prepareUpdateParameters(PreparedStatement ps, long id, OperationTask.Update update)
        throws JsonProcessingException, SQLException
    {
        var i = 0;
        if (update.status() != null) {
            ps.setString(++i, update.status().name());
        }
        if (update.metadata() != null) {
            ps.setString(++i, objectMapper.writeValueAsString(update.metadata()));
        }
        if (update.operationId() != null) {
            ps.setString(++i, update.operationId());
        }
        ps.setLong(++i, id);
    }

    private OperationTask readTask(ResultSet rs) throws SQLException {
        try {
            var id = rs.getLong("id");
            var name = rs.getString("name");
            var entityId = rs.getString("entity_id");
            var type = rs.getString("type");
            var status = OperationTask.Status.valueOf(rs.getString("status"));
            var createdAt = rs.getTimestamp("created_at").toInstant();
            var updatedAt = rs.getTimestamp("updated_at").toInstant();
            var metadata = objectMapper.readValue(rs.getString("metadata"),
                MAP_TYPE_REFERENCE);
            var operationId = rs.getString("operation_id");
            var workerId = rs.getString("worker_id");
            var leaseTillTs = rs.getTimestamp("lease_till");
            var leaseTill = leaseTillTs != null ? leaseTillTs.toInstant() : null;
            return new OperationTask(id, name, entityId, type, status, createdAt, updatedAt, metadata, operationId,
                workerId,
                leaseTill);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot read metadata for operation task object", e);
        }

    }
}
