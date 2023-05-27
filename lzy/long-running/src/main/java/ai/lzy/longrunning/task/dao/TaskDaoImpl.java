package ai.lzy.longrunning.task.dao;

import ai.lzy.longrunning.task.Task;
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

public class TaskDaoImpl implements TaskDao {

    private static final String FIELDS = "id, name, entity_id, type, status, created_at, updated_at, metadata," +
        " operation_id, worker_id, lease_till";
    public static final String SELECT_QUERY = "SELECT %s FROM task WHERE id = ?".formatted(FIELDS);
    public static final String INSERT_QUERY = """
        INSERT INTO task (name, entity_id, type, status, created_at, updated_at, metadata, operation_id)
        VALUES (?, ?, cast(? as task_type), cast(? as task_status), now(), now(), cast(? as jsonb), ?)
        RETURNING %s;
        """.formatted(FIELDS);

    //in first nested request we gather all tasks that either locked or free.
    //in second nested request we filter result of previous request to get only free tasks
    // and select only specific amount.
    private static final String LOCK_PENDING_BATCH_QUERY = """
        UPDATE task
        SET status = 'RUNNING', worker_id = ?, updated_at = now(), lease_till = now() + cast(? as interval)
        WHERE id IN (
            SELECT id
            FROM task
            WHERE id IN (
                SELECT DISTINCT ON (entity_id) id
                FROM task
                WHERE status IN ('PENDING', 'RUNNING')
                ORDER BY entity_id, id
            ) AND status = 'PENDING'
            LIMIT ?
        )
        RETURNING %s;
        """.formatted(FIELDS);
    public static final String DELETE_QUERY = "DELETE FROM task WHERE id = ?";
    public static final String UPDATE_LEASE_QUERY = """
        UPDATE task
        SET lease_till = now() + cast(? as interval)
        WHERE id = ?
        RETURNING %s
        """.formatted(FIELDS);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() { };


    private final Storage storage;
    private final ObjectMapper objectMapper;

    public TaskDaoImpl(Storage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Nullable
    @Override
    public Task get(long id, @Nullable TransactionHandle tx) throws SQLException {
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
    public Task update(long id, Task.Update update, @Nullable TransactionHandle tx) throws SQLException {
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
    public Task updateLease(long id, Duration duration, @Nullable TransactionHandle tx) throws SQLException {
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
    public Task insert(Task task, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(INSERT_QUERY)) {
                var i = 0;
                ps.setString(++i, task.name());
                ps.setString(++i, task.entityId());
                ps.setString(++i, task.type());
                ps.setString(++i, task.status().name());
                ps.setString(++i, objectMapper.writeValueAsString(task.metadata()));
                ps.setString(++i, task.operationId());
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
    public List<Task> lockPendingBatch(String ownerId, Duration leaseTime, int batchSize,
                                       @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, c -> {
            try (PreparedStatement ps = c.prepareStatement(LOCK_PENDING_BATCH_QUERY)) {
                var i = 0;
                ps.setString(++i, ownerId);
                ps.setString(++i, leaseTime.toString());
                ps.setInt(++i, batchSize);
                try (var rs = ps.executeQuery()) {
                    var result = new ArrayList<Task>(rs.getFetchSize());
                    while (rs.next()) {
                        result.add(readTask(rs));
                    }
                    return result;
                }
            }
        });
    }

    private static String prepareUpdateQuery(Task.Update update) {
        var sb = new StringBuilder("UPDATE task SET ");
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

    private void prepareUpdateParameters(PreparedStatement ps, long id, Task.Update update)
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

    private Task readTask(ResultSet rs) throws SQLException {
        try {
            var id = rs.getLong("id");
            var name = rs.getString("name");
            var entityId = rs.getString("entity_id");
            var type = rs.getString("type");
            var status = Task.Status.valueOf(rs.getString("status"));
            var createdAt = rs.getTimestamp("created_at").toInstant();
            var updatedAt = rs.getTimestamp("updated_at").toInstant();
            var metadata = objectMapper.readValue(rs.getString("metadata"), MAP_TYPE_REFERENCE);
            var operationId = rs.getString("operation_id");
            var workerId = rs.getString("worker_id");
            var leaseTillTs = rs.getTimestamp("lease_till");
            var leaseTill = leaseTillTs != null ? leaseTillTs.toInstant() : null;
            return new Task(id, name, entityId, type, status, createdAt, updatedAt, metadata, operationId, workerId,
                leaseTill);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot read metadata for task object", e);
        }

    }
}
