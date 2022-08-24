package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.model.Operation;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import jakarta.inject.Singleton;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class OperationDaoImpl implements OperationDao {
    private static final Logger LOG = LogManager.getLogger(OperationDaoImpl.class);
    private static final String FIELDS = " id, meta, created_by, created_at, modified_at, description,"
        + " done, response, \"error\" ";
    private final Storage storage;

    public OperationDaoImpl(AllocatorDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Operation create(String description, String createdBy, Any meta, @Nullable TransactionHandle transaction) {
        final var op = new Operation(UUID.randomUUID().toString(), meta, createdBy, Instant.now(), Instant.now(),
            description, false, null, null);
        LOG.info("Operation {} is creating", op);
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "INSERT INTO operation (" + FIELDS + """
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
                writeOp(op, s);
                s.execute();
            }
        });
        LOG.info("Operation with id={} has been created", op.id());
        return op;
    }

    @Nullable
    @Override
    public Operation get(String opId, @Nullable TransactionHandle transaction) {
        final Operation[] op = new Operation[1];
        LOG.info("Getting op with id={}", opId);
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "SELECT" + FIELDS + """
                FROM operation
                WHERE id = ?
                """)) {
                s.setString(1, opId);
                final var res = s.executeQuery();
                if (!res.next()) {
                    op[0] = null;
                    LOG.info("Op with id={} not found in the storage", opId);
                    return;
                }
                final var id = res.getString(1);
                final var meta = Any.parseFrom(res.getBytes(2));
                final var createdBy = res.getString(3);
                final var createdAt = res.getTimestamp(4).toInstant();
                final var modifiedAt = res.getTimestamp(5).toInstant();
                final var description = res.getString(6);
                final var done = res.getBoolean(7);
                final var responseBytes = res.getBytes(8);
                final Any response;
                if (responseBytes == null) {
                    response = null;
                } else {
                    response = Any.parseFrom(responseBytes);
                }
                final var errorBytes = res.getBytes(9);
                final io.grpc.Status error;
                if (errorBytes == null) {
                    error = null;
                } else {
                    final var status = Status.parseFrom(errorBytes);
                    error = io.grpc.Status.fromCodeValue(status.getCode())
                        .withDescription(status.getMessage());
                }

                op[0] = new Operation(id, meta, createdBy, createdAt, modifiedAt, description, done, response, error);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("Cannot parse proto", e);
            }
        });
        LOG.info("Return op={}", op[0]);
        return op[0];
    }

    @Override
    public void update(Operation op, @Nullable TransactionHandle transaction) {
        LOG.info("Operation {} is updating", op);
        DbOperation.execute(transaction, storage, con -> {
            try (final var s = con.prepareStatement(
                "UPDATE operation SET (" + FIELDS + """
                ) = (?, ?, ?, ?, ?, ?, ?, ?, ?)
                WHERE id = ?
                """)) {
                s.setString(10, op.id());
                writeOp(op, s);
                s.execute();
            }
        });
        LOG.info("Operation with id={} has been updated", op.id());
    }

    private void writeOp(Operation op, PreparedStatement s) throws SQLException {
        s.setString(1, op.id());
        s.setBytes(2, op.meta().toByteArray());
        s.setString(3, op.createdBy());
        s.setTimestamp(4, Timestamp.from(op.createdAt()));
        s.setTimestamp(5, Timestamp.from(op.modifiedAt()));
        s.setString(6, op.description());
        s.setBoolean(7, op.done());
        s.setBytes(8, op.response() == null ? null : op.response().toByteArray());
        if (op.error() != null) {
            final var status = Status.newBuilder()
                    .setCode(op.error().getCode().value());
            if (op.error().getDescription() != null) {
                status.setMessage(op.error().getDescription());
            }
            s.setBytes(9, status.build().toByteArray());
        } else {
            s.setBytes(9, null);
        }
    }
}
