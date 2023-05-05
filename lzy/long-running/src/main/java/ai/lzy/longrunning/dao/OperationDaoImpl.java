package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class OperationDaoImpl implements OperationDao {
    private static final Logger LOG = LogManager.getLogger(OperationDaoImpl.class);

    private static final List<String> FIELDS = List.of("id", "meta", "created_by", "created_at",
        "modified_at", "description", "deadline", "done", "response", "error", "idempotency_key", "request_hash");

    private static final String FIELDS_STRING = String.join(", ", FIELDS);

    private static final String QUERY_CREATE_OPERATION = """
        INSERT INTO operation (%s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".formatted(FIELDS_STRING);

    private static final String QUERY_GET_OPERATION = """
        SELECT %s
        FROM operation
        WHERE id = ?""".formatted(FIELDS_STRING);

    private static final String QUERY_FIND_OPERATION = """
        SELECT %s
        FROM operation
        WHERE idempotency_key = ?""".formatted(FIELDS_STRING);

    private static final String QUERY_UPDATE_OPERATION_META_RESPONSE = """
        WITH
            prev AS (
                SELECT %s FROM operation WHERE id = ? FOR UPDATE
            ),
            new AS (
                UPDATE operation
                SET meta = ?, response = ?, done = TRUE, modified_at = NOW()
                WHERE id = ? AND done = FALSE
                RETURNING id, modified_at
            )
        SELECT (new.id IS NOT NULL) AS __updated__,
               prev.*,
               (CASE WHEN (new.id IS NOT NULL) THEN new.modified_at ELSE prev.modified_at END) AS modified_at
        FROM prev LEFT JOIN new USING (id)""".formatted(FIELDS_STRING);

    private static final String QUERY_UPDATE_OPERATION_RESPONSE = """
        WITH
            prev AS (
                SELECT %s FROM operation WHERE id = ? FOR UPDATE
            ),
            new AS (
                UPDATE operation
                SET response = ?, done = TRUE, modified_at = NOW()
                WHERE id = ? AND done = FALSE
                RETURNING id, modified_at
            )
        SELECT (new.id IS NOT NULL) AS __updated__,
               prev.*,
               (CASE WHEN (new.id IS NOT NULL) THEN new.modified_at ELSE prev.modified_at END) AS modified_at
        FROM prev LEFT JOIN new USING (id)""".formatted(FIELDS_STRING);

    private static final String QUERY_UPDATE_OPERATION_META = """
        WITH
            prev AS (
                SELECT %s FROM operation WHERE id = ? FOR UPDATE
            ),
            new AS (
                UPDATE operation
                SET meta = ?, modified_at = NOW()
                WHERE id = ? AND done = FALSE
                RETURNING id, modified_at
            )
        SELECT (new.id IS NOT NULL) AS __updated__,
               prev.*,
               (CASE WHEN (new.id IS NOT NULL) THEN new.modified_at ELSE prev.modified_at END) AS modified_at
        FROM prev LEFT JOIN new USING (id)""".formatted(FIELDS_STRING);

    private static final String QUERY_UPDATE_OPERATION_ERROR = """
        WITH
            prev AS (
                SELECT %s FROM operation WHERE id = ? FOR UPDATE
            ),
            new AS (
                UPDATE operation
                SET error = ?, done = TRUE, modified_at = NOW()
                WHERE id = ? AND done = FALSE
                RETURNING id, modified_at
            )
        SELECT (new.id IS NOT NULL) AS __updated__,
               prev.*,
               (CASE WHEN (new.id IS NOT NULL) THEN new.modified_at ELSE prev.modified_at END) AS modified_at
        FROM prev LEFT JOIN new USING (id)""".formatted(FIELDS_STRING);

    private static final String QUERY_UPDATE_OPERATION_TIME = """
        WITH
            prev AS (
                SELECT id FROM operation WHERE id = ? FOR UPDATE
            ),
            new AS (
                UPDATE operation
                SET modified_at = NOW()
                WHERE id = ? AND done = FALSE
                RETURNING id
            )
        SELECT
            (new.id IS NOT NULL) AS __updated__
        FROM prev LEFT JOIN new USING (id)""";

    private static final String QUERY_DELETE_COMPLETED_OPERATION = """
        DELETE FROM operation
        WHERE id = ?
          AND done = TRUE""";

    private static final String QUERY_DELETE_OUTDATED_OPERATIONS = """
        DELETE FROM operation
        WHERE done = TRUE
          AND modified_at + INTERVAL '%d hours' < NOW()""";

    private static final String QUERY_CANCEL_OPERATIONS = """
        UPDATE operation
        SET error = ?, done = TRUE, modified_at = NOW()
        WHERE id IN ? AND done = FALSE
        """;

    private final Storage storage;

    public OperationDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void create(Operation operation, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Create operation {}", operation.toShortString());

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_CREATE_OPERATION)) {
                st.setString(1, operation.id());

                var meta = operation.meta();

                if (meta != null) {
                    st.setBytes(2, meta.toByteArray());
                } else {
                    st.setBytes(2, null);
                }

                st.setString(3, operation.createdBy());
                st.setTimestamp(4, Timestamp.from(operation.createdAt()));
                st.setTimestamp(5, Timestamp.from(operation.modifiedAt()));
                st.setString(6, operation.description());

                var deadline = operation.deadline();
                st.setTimestamp(7, deadline != null ? Timestamp.from(deadline) : null);

                st.setBoolean(8, operation.done());

                var response = operation.response();
                if (response != null) {
                    st.setBytes(9, response.toByteArray());
                } else {
                    st.setBytes(9, null);
                }

                var error = operation.error();
                if (error != null) {
                    var status = Status.newBuilder().setCode(error.getCode().value());
                    String description = error.getDescription();
                    if (description != null) {
                        status.setMessage(description);
                    }
                    st.setBytes(10, status.build().toByteArray());
                } else {
                    st.setBytes(10, null);
                }

                Operation.IdempotencyKey idempotencyKey = operation.idempotencyKey();
                if (idempotencyKey != null) {
                    st.setString(11, idempotencyKey.token());
                    st.setString(12, idempotencyKey.requestHash());
                } else {
                    st.setString(11, null);
                    st.setString(12, null);
                }

                st.execute();
            }
        });

        LOG.debug("Operation {} has been created", operation.id());
    }

    @Nullable
    @Override
    public Operation getByIdempotencyKey(String idempotencyKey, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        var operation = getBy(idempotencyKey, QUERY_FIND_OPERATION, transaction);

        if (operation == null) {
            LOG.debug("Cannot find operation by { idempotencyKey: {} }", idempotencyKey);
        }

        return operation;
    }

    @Nullable
    @Override
    public Operation get(String operationId, @Nullable TransactionHandle transaction) throws SQLException {
        var operation = getBy(operationId, QUERY_GET_OPERATION, transaction);

        if (operation == null) {
            LOG.debug("Cannot find operation by { operationId: {} }", operationId);
        }

        return operation;
    }

    @Nullable
    private Operation getBy(String key, String sql, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Get operation {}", key);
        return DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(sql + forUpdate(transaction))) {
                st.setString(1, key);

                var rs = st.executeQuery();
                if (rs.next()) {
                    return from(rs);
                }

                return null;
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("Cannot parse proto", e);
            }
        });
    }

    @Override
    public void update(String id, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Update operation {}", id);

        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement st = con.prepareStatement(QUERY_UPDATE_OPERATION_TIME)) {
                st.setString(1, id);
                st.setString(2, id);

                var rs = st.executeQuery();

                if (rs.next()) {
                    if (rs.getBoolean("__updated__")) {
                        LOG.info("Operation {} successfully updated", id);
                    } else {
                        LOG.warn("Operation {} already completed", id);
                        throw new OperationCompletedException(id);
                    }
                } else {
                    LOG.warn("Operation {} not exists", id);
                    throw new NotFoundException("Operation %s not found".formatted(id));
                }
            }
        });
    }

    @Override
    public Operation complete(String id, @Nullable Any meta, Any response, @Nullable TransactionHandle tx)
        throws SQLException
    {
        LOG.info("Complete operation {}", id);

        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement st = con.prepareStatement(
                meta != null ? QUERY_UPDATE_OPERATION_META_RESPONSE : QUERY_UPDATE_OPERATION_RESPONSE))
            {
                int index = 0;
                st.setString(++index, id);
                if (meta != null) {
                    st.setBytes(++index, meta.toByteArray());
                }
                st.setBytes(++index, response.toByteArray());
                st.setString(++index, id);

                var rs = st.executeQuery();
                var op = processResult(id, rs, "completed");
                if (meta != null) {
                    op.modifyMeta(meta);
                }
                op.setResponse(response);
                return op;
            }
        });
    }

    @Override
    public Operation complete(String id, Any response, @Nullable TransactionHandle transaction) throws SQLException {
        return complete(id, null, response, transaction);
    }

    @Override
    public Operation updateMeta(String id, Any meta, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Update operation {} meta", id);

        return DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement st = con.prepareStatement(QUERY_UPDATE_OPERATION_META)) {
                st.setString(1, id);
                st.setBytes(2, meta.toByteArray());
                st.setString(3, id);

                var rs = st.executeQuery();
                var op = processResult(id, rs, "updated");
                op.modifyMeta(meta);
                return op;
            }
        });
    }

    @Override
    public Operation fail(String id, Status error, TransactionHandle transaction) throws SQLException {
        LOG.info("Update operation with error: { operationId: {} }", id);

        return DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement st = con.prepareStatement(QUERY_UPDATE_OPERATION_ERROR)) {
                st.setString(1, id);
                st.setBytes(2, error.toByteArray());
                st.setString(3, id);

                var rs = st.executeQuery();
                var op = processResult(id, rs, "failed");
                op.setError(io.grpc.Status.fromCodeValue(error.getCode()).withDescription(error.getMessage()));
                return op;
            }
        });
    }

    @Override
    public void cancel(Collection<String> ids, Status error, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Cancel operations {}", String.join(", ", ids));

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_CANCEL_OPERATIONS)) {
                statement.setBytes(1, error.toByteArray());
                statement.setString(2, "(" + String.join(", ", ids) + ")");
                statement.executeUpdate();
            }
        });
    }

    @Override
    public boolean deleteCompletedOperation(String operationId, TransactionHandle transaction) throws SQLException {
        LOG.debug("Delete completed operation {}", operationId);
        return DbOperation.execute(transaction, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_DELETE_COMPLETED_OPERATION)) {
                st.setString(1, operationId);
                return st.executeUpdate() != 0;
            }
        });
    }

    @Override
    public int deleteOutdatedOperations(int hours) throws SQLException {
        LOG.debug("Delete outdated operations (more then {} hours)", hours);
        return DbOperation.execute(null, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_DELETE_OUTDATED_OPERATIONS.formatted(hours))) {
                return st.executeUpdate();
            }
        });
    }

    private static Operation processResult(String id, ResultSet rs, String action) throws SQLException {
        if (rs.next()) {
            if (rs.getBoolean("__updated__")) {
                LOG.info("Operation {} successfully {}", id, action);
                try {
                    return from(rs);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Cannot load operation %s".formatted(id), e);
                }
            } else {
                LOG.warn("Operation {} concurrently updated", id);
                throw new OperationCompletedException(id);
            }
        } else {
            LOG.warn("Operation {} not exists", id);
            throw new NotFoundException("Operation %s not found".formatted(id));
        }
    }

    private static Operation from(ResultSet resultSet) throws InvalidProtocolBufferException, SQLException {
        var id = resultSet.getString("id");
        var createdBy = resultSet.getString("created_by");
        var createdAt = resultSet.getTimestamp("created_at").toInstant();
        var description = resultSet.getString("description");
        var metaBytes = resultSet.getBytes("meta");
        var modifiedAt = resultSet.getTimestamp("modified_at").toInstant();
        var deadline = Optional.ofNullable(resultSet.getTimestamp("deadline")).map(Timestamp::toInstant).orElse(null);
        var done = resultSet.getBoolean("done");
        var responseBytes = resultSet.getBytes("response");
        var errorBytes = resultSet.getBytes("error");
        var idempotencyToken = resultSet.getString("idempotency_key");
        var requestChecksum = resultSet.getString("request_hash");

        Any meta = null;
        Any response = null;
        io.grpc.Status error = null;

        if (metaBytes != null) {
            meta = Any.parseFrom(metaBytes);
        }

        if (responseBytes != null) {
            response = Any.parseFrom(responseBytes);
        }

        if (errorBytes != null) {
            var status = Status.parseFrom(errorBytes);
            error = io.grpc.Status.fromCodeValue(status.getCode()).withDescription(status.getMessage());
        }

        Operation.IdempotencyKey idempotencyKey = null;

        if (idempotencyToken != null) {
            idempotencyKey = new Operation.IdempotencyKey(idempotencyToken, requestChecksum);
        }

        return new Operation(id, createdBy, createdAt, description, deadline, idempotencyKey, meta, modifiedAt, done,
            response, error);
    }

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
