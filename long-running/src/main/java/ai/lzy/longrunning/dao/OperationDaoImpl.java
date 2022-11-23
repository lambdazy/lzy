package ai.lzy.longrunning.dao;

import ai.lzy.longrunning.Operation;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nullable;

public class OperationDaoImpl implements OperationDao {
    private static final Logger LOG = LogManager.getLogger(OperationDaoImpl.class);

    private static final List<String> FIELDS = List.of("id", "meta", "created_by", "created_at",
        "modified_at", "description", "done", "response", "error", "idempotency_key", "request_hash");

    private static final String QUERY_CREATE_OPERATION = """
        INSERT INTO operation (%s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".formatted(String.join(", ", FIELDS));

    private static final String QUERY_GET_OPERATION = """
        SELECT %s
        FROM operation
        WHERE id = ?""".formatted(String.join(", ", FIELDS));

    private static final String QUERY_FIND_OPERATION = """
        SELECT %s
        FROM operation
        WHERE idempotency_key = ?""".formatted(String.join(", ", FIELDS));

    private static final String QUERY_UPDATE_OPERATION_META_RESPONSE = """
        UPDATE operation
        SET (meta, response, done, modified_at) = (?, ?, ?, ?)
        WHERE id = ?""";

    private static final String QUERY_UPDATE_OPERATION_META = """
        UPDATE operation
        SET (meta, modified_at) = (?, ?)
        WHERE id = ?""";

    private static final String QUERY_UPDATE_OPERATION_RESPONSE = """
        UPDATE operation
        SET (response, done, modified_at) = (?, ?, ?)
        WHERE id = ?""";

    private static final String QUERY_UPDATE_OPERATION_ERROR = """
        UPDATE operation
        SET (error, done, modified_at) = (?, ?, ?)
        WHERE id = ?""";

    private final Storage storage;

    public OperationDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void create(Operation operation, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Create operation {}", operation.toShortString());

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_CREATE_OPERATION)) {
                statement.setString(1, operation.id());

                var meta = operation.meta();

                if (meta != null) {
                    statement.setBytes(2, meta.toByteArray());
                } else {
                    statement.setBytes(2, null);
                }

                statement.setString(3, operation.createdBy());
                statement.setTimestamp(4, Timestamp.from(operation.createdAt()));
                statement.setTimestamp(5, Timestamp.from(operation.modifiedAt()));
                statement.setString(6, operation.description());
                statement.setBoolean(7, operation.done());

                var response = operation.response();

                if (response != null) {
                    statement.setBytes(8, response.toByteArray());
                } else {
                    statement.setBytes(8, null);
                }

                var error = operation.error();

                if (error != null) {
                    var status = Status.newBuilder().setCode(error.getCode().value());
                    String description = error.getDescription();
                    if (description != null) {
                        status.setMessage(description);
                    }
                    statement.setBytes(9, status.build().toByteArray());
                } else {
                    statement.setBytes(9, null);
                }

                Operation.IdempotencyKey idempotencyKey = operation.idempotencyKey();
                if (idempotencyKey != null) {
                    statement.setString(10, idempotencyKey.token());
                    statement.setString(11, idempotencyKey.requestHash());
                } else {
                    statement.setString(10, null);
                    statement.setString(11, null);
                }

                statement.execute();
            }
        });

        LOG.info("Operation {} has been created", operation.id());
    }

    @Nullable
    @Override
    public Operation getByIdempotencyKey(String idempotencyKey, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        var operation = getBy(idempotencyKey, QUERY_FIND_OPERATION, transaction);

        if (operation == null) {
            LOG.info("Cannot find operation by { idempotencyKey: {} }", idempotencyKey);
        }

        return operation;
    }

    @Nullable
    @Override
    public Operation get(String operationId, @Nullable TransactionHandle transaction) throws SQLException {
        var operation = getBy(operationId, QUERY_GET_OPERATION, transaction);

        if (operation == null) {
            LOG.info("Cannot find operation by { operationId: {} }", operationId);
        }

        return operation;
    }

    private Operation getBy(String key, String sql, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        Operation[] result = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(sql + forUpdate(transaction))) {
                statement.setString(1, key);

                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    result[0] = from(rs);
                }
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("Cannot parse proto", e);
            }
        });

        return result[0];
    }

    @Nullable
    @Override
    public Operation updateMetaAndResponse(String id, byte[] meta, byte[] response,
                                           @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.info("Update operation meta and response: { operationId: {} }", id);

        Operation[] result = {null};

        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_OPERATION_META_RESPONSE,
                Statement.RETURN_GENERATED_KEYS))
            {
                statement.setBytes(1, meta);
                statement.setBytes(2, response);
                statement.setBoolean(3, true);
                statement.setTimestamp(4, Timestamp.from(Instant.now()));
                statement.setString(5, id);

                statement.execute();

                try (ResultSet rs = statement.getGeneratedKeys()) {
                    if (rs.next()) {
                        result[0] = from(rs);
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Cannot parse proto", e);
                }
            }
        });

        if (result[0] != null) {
            LOG.info("Operation meta and response has been updated: { operationId: {} }", id);
        } else {
            LOG.warn("Operation not found: { operationId: {} }", id);
        }

        return result[0];
    }

    @Override
    @Nullable
    public Operation updateMeta(String id, byte[] meta, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Update operation meta: { operationId: {} }", id);

        Operation[] result = {null};

        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_OPERATION_META, Statement.RETURN_GENERATED_KEYS)) {
                statement.setBytes(1, meta);
                statement.setTimestamp(2, Timestamp.from(Instant.now()));
                statement.setString(3, id);

                statement.execute();

                try (ResultSet rs = statement.getGeneratedKeys()) {
                    if (rs.next()) {
                        result[0] = from(rs);
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Cannot parse proto", e);
                }
            }
        });

        if (result[0] != null) {
            LOG.info("Operation meta has been updated: { operationId: {} }", id);
        } else {
            LOG.warn("Operation not found: { operationId: {} }", id);
        }

        return result[0];
    }

    @Override
    @Nullable
    public Operation updateResponse(String id, byte[] response, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Update operation with response: { operationId: {} }", id);

        return updateAsDone(id, response, QUERY_UPDATE_OPERATION_RESPONSE, transaction);
    }

    @Override
    @Nullable
    public Operation updateError(String id, byte[] error, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.info("Update operation with error: { operationId: {} }", id);

        return updateAsDone(id, error, QUERY_UPDATE_OPERATION_ERROR, transaction);
    }

    private Operation updateAsDone(String id, byte[] opResult, String sql, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        Operation[] result = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                statement.setBytes(1, opResult);
                statement.setBoolean(2, true);
                statement.setTimestamp(3, Timestamp.from(Instant.now()));
                statement.setString(4, id);

                statement.execute();

                try (ResultSet rs = statement.getGeneratedKeys()) {
                    if (rs.next()) {
                        result[0] = from(rs);
                    }
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Cannot parse proto", e);
                }
            }
        });

        if (result[0] != null) {
            LOG.info("Operation has been updated: { operationId: {} }", id);
        } else {
            LOG.warn("Operation not found: { operationId: {} }", id);
        }

        return result[0];
    }

    private Operation from(ResultSet resultSet) throws InvalidProtocolBufferException, SQLException {
        var id = resultSet.getString("id");
        var createdBy = resultSet.getString("created_by");
        var createdAt = resultSet.getTimestamp("created_at").toInstant();
        var description = resultSet.getString("description");
        var metaBytes = resultSet.getBytes("meta");
        var modifiedAt = resultSet.getTimestamp("modified_at").toInstant();
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

        return new Operation(id, createdBy, createdAt, description, idempotencyKey, meta, modifiedAt, done, response,
            error);
    }

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
