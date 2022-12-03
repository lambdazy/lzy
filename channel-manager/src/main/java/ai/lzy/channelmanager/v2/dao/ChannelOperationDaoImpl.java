package ai.lzy.channelmanager.v2.dao;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.operation.ChannelOperation;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nullable;

public class ChannelOperationDaoImpl implements ChannelOperationDao {

    private static final String QUERY_CREATE_OP = """
        INSERT INTO channel_operation (op_id, started_at, deadline, op_type, state_json)
        VALUES (?, ?, ?, ?::channel_operation_type, ?)""";

    private static final String QUERY_UPDATE_OP = """
        UPDATE channel_operation
        SET state_json = ?
        WHERE op_id = ?""";

    private static final String QUERY_DELETE_OP = """
        DELETE FROM channel_operation
        WHERE op_id = ?""";

    private static final String QUERY_FAIL_OP = """
        UPDATE channel_operation
        SET failed = TRUE, fail_reason = ?
        WHERE op_id = ?""";

    private final ChannelManagerDataSource storage;

    public ChannelOperationDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void create(ChannelOperation operation, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_CREATE_OP)) {
                int index = 0;
                st.setString(++index, operation.id());
                st.setTimestamp(++index, Timestamp.from(operation.startedAt().truncatedTo(ChronoUnit.MILLIS)));
                st.setTimestamp(++index, Timestamp.from(operation.deadline().truncatedTo(ChronoUnit.MILLIS)));
                st.setString(++index, operation.type().name());
                st.setString(++index, operation.stateJson());
                st.executeUpdate();
            }
        });
    }

    @Override
    public void update(String operationId, String stateJson, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_UPDATE_OP)) {
                int index = 0;
                st.setString(++index, stateJson);
                st.setString(++index, operationId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void delete(String operationId, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_DELETE_OP)) {
                int index = 0;
                st.setString(++index, operationId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void fail(String operationId, String reason, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_FAIL_OP)) {
                int index = 0;
                st.setString(++index, reason);
                st.setString(++index, operationId);
                st.executeUpdate();
            }
        });
    }
}
