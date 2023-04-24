package ai.lzy.allocator.disk.dao;

import ai.lzy.allocator.disk.DiskOperation;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.NotNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class DiskOpDao {
    private static final Logger LOG = LogManager.getLogger(DiskOpDao.class);

    private static final String QUERY_CREATE_DISK_OP = """
        INSERT INTO disk_op (op_id, started_at, deadline, owner_instance, op_type, state_json, reqid, description)
        VALUES (?, ?, ?, ?, ?::disk_operation_type, ?, ?, ?)""";

    private static final String QUERY_UPDATE_DISK_OP = """
        UPDATE disk_op
        SET state_json = ?
        WHERE op_id = ?""";

    private static final String QUERY_DELETE_DISK_OP = """
        DELETE FROM disk_op
        WHERE op_id = ?""";

    private static final String QUERY_GET_DISK_OP = """
        SELECT started_at, deadline, owner_instance, op_type::TEXT, state_json, reqid, description
        FROM disk_op
        WHERE op_id = ? AND NOT failed""";

    private static final String QUERY_FAIL_DISK_OP = """
        UPDATE disk_op
        SET failed = TRUE, fail_reason = ?
        WHERE op_id = ?""";

    private static final String QUERY_GET_ACTIVE_DISK_OPS = """
        SELECT op_id, started_at, deadline, owner_instance, op_type::TEXT, state_json, reqid, description
        FROM disk_op
        WHERE owner_instance = ? AND NOT failed""";

    private final AllocatorDataSource storage;

    @Inject
    public DiskOpDao(AllocatorDataSource storage) {
        this.storage = storage;
    }

    public void createDiskOp(DiskOperation diskOp, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_CREATE_DISK_OP)) {
                int idx = 0;
                st.setString(++idx, diskOp.opId());
                st.setTimestamp(++idx, Timestamp.from(diskOp.startedAt().truncatedTo(ChronoUnit.SECONDS)));
                st.setTimestamp(++idx, Timestamp.from(diskOp.deadline().truncatedTo(ChronoUnit.SECONDS)));
                st.setString(++idx, diskOp.ownerInstanceId());
                st.setString(++idx, diskOp.diskOpType().name());
                st.setString(++idx, diskOp.state());
                st.setString(++idx, diskOp.reqid());
                st.setString(++idx, diskOp.descr());
                st.executeUpdate();
            }
        });
    }

    public void updateDiskOp(String opId, String newState, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_UPDATE_DISK_OP)) {
                st.setString(1, newState);
                st.setString(2, opId);
                st.executeUpdate();
            }
        });
    }

    public boolean deleteDiskOp(String opId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_DELETE_DISK_OP)) {
                st.setString(1, opId);
                return st.executeUpdate() > 0;
            }
        });
    }

    @Nullable
    public DiskOperation getDiskOp(String opId, @Nullable TransactionHandle tx) throws SQLException {
        final DiskOperation[] ref = {null};
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_GET_DISK_OP)) {
                st.setString(1, opId);

                var rs = st.executeQuery();
                if (rs.next()) {
                    ref[0] = readDiskOp(rs);
                }
            }
        });
        return ref[0];
    }

    public void failDiskOp(String opId, String reason, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_FAIL_DISK_OP)) {
                st.setString(1, reason);
                st.setString(2, opId);
                st.executeUpdate();
            }
        });
    }

    public List<DiskOperation> getActiveDiskOps(String ownerInstanceId, @Nullable TransactionHandle tx)
        throws SQLException
    {
        final List<DiskOperation> ops = new ArrayList<>();
        DbOperation.execute(tx, storage, conn -> {
            try (PreparedStatement st = conn.prepareStatement(QUERY_GET_ACTIVE_DISK_OPS)) {
                st.setString(1, ownerInstanceId);
                var rs = st.executeQuery();
                while (rs.next()) {
                    ops.add(readDiskOp(rs));
                }
            }
        });
        return ops;
    }

    @NotNull
    private static DiskOperation readDiskOp(ResultSet rs) throws SQLException {
        return new DiskOperation(
            rs.getString(1),
            rs.getString(8),
            rs.getTimestamp(2).toInstant(),
            rs.getTimestamp(3).toInstant(),
            rs.getString(4),
            DiskOperation.Type.valueOf(rs.getString(5)),
            rs.getString(6),
            rs.getString(7),
            null);
    }
}
