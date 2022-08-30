package ai.lzy.allocator.dao.impl;

import ai.lzy.allocator.dao.OperationDao;
import ai.lzy.allocator.model.Operation;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@Singleton
public class OperationDaoImpl implements OperationDao {
    private static final Logger LOG = LogManager.getLogger(OperationDaoImpl.class);

    private static final String FIELDS = " id, meta, created_by," +
                                         " created_at, modified_at, description," +
                                         " done, response, \"error\" ";

    private static final String QUERY_CREATE_OPERATION = """
        INSERT INTO operation (%s)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""".formatted(FIELDS);

    private static final String QUERY_READ_OPERATION = """
        SELECT %s
        FROM operation
        WHERE id = ?""".formatted(FIELDS);

    private static final String QUERY_UPDATE_OPERATION = """
        UPDATE operation
        SET (%s) = (?, ?, ?, ?, ?, ?, ?, ?, ?)
        WHERE id = ?""".formatted(FIELDS);

    private final Storage storage;

    @Inject
    public OperationDaoImpl(AllocatorDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Operation create(String opId, String description, String createdBy, Any meta, @Nullable TransactionHandle th)
        throws SQLException
    {
        final var op = Operation.create(opId, createdBy, description, meta);

        LOG.info("Create operation {}", op.toShortString());

        DbOperation.execute(th, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_CREATE_OPERATION)) {
                writeOp(op, s);
                s.execute();
            }
        });

        LOG.info("Operation {} has been created", op.id());
        return op;
    }

    @Nullable
    @Override
    public Operation get(String opId, @Nullable TransactionHandle th) throws SQLException {
        LOG.info("Get op {}", opId);

        final Operation[] op = {null};

        DbOperation.execute(th, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_READ_OPERATION + forUpdate(th))) {
                s.setString(1, opId);

                final var res = s.executeQuery();
                if (!res.next()) {
                    op[0] = null;
                    LOG.info("Op {} not found in the storage", opId);
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
                final Any response = responseBytes == null ? null : Any.parseFrom(responseBytes);
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

        LOG.info("Return op {}", op[0]);
        return op[0];
    }

    @Override
    public void update(Operation op, @Nullable TransactionHandle th) throws SQLException {
        LOG.info("Update operation {}", op);

        DbOperation.execute(th, storage, con -> {
            try (final var s = con.prepareStatement(QUERY_UPDATE_OPERATION)) {
                s.setString(10, op.id());
                writeOp(op, s);
                s.execute();
            }
        });

        LOG.info("Operation {} has been updated", op.id());
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

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
