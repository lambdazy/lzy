package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.dao.ExecutionOperationsDao;
import ai.lzy.util.grpc.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Singleton
public class ExecutionOperationsDaoImpl implements ExecutionOperationsDao {
    private static final Logger LOG = LogManager.getLogger(ExecutionOperationsDaoImpl.class);

    private static final String QUERY_INSERT_OPERATION = """
        INSERT INTO execution_operations (op_id, op_type, service_instance_id, execution_id)
        VALUES (?, ?, ?, ?)""";

    private static final String QUERY_SELECT_EXEC_OPERATIONS = """
        SELECT op_id, op_type, execution_id FROM execution_operations WHERE execution_id = ?""";

    public static final String QUERY_DELETE_EXEC_OPERATION = """
        DELETE FROM execution_operations WHERE op_id = ?""";

    public static final String QUERY_DELETE_EXEC_OPERATIONS = """
        DELETE FROM execution_operations WHERE op_id = ANY (?)""";

    private static final String QUERY_UPDATE_EXECUTE_GRAPH_OP_STATE = """
        UPDATE execution_operations SET state_json = ? WHERE op_id = ?""";

    private final LzyServiceStorage storage;
    private final ObjectMapper objectMapper;

    public ExecutionOperationsDaoImpl(LzyServiceStorage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void createStartOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Create start execution operation in storage: { opId: {}, execId: {} }", opId, execId);
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_INSERT_OPERATION)) {
                st.setString(1, opId);
                st.setString(2, OpType.START_EXECUTION.toString());
                st.setString(3, instanceId);
                st.setString(4, execId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void createStopOp(String opId, String instanceId, String execId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Create stop execution operation in storage: { opId: {}, execId: {} }", opId, execId);
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_INSERT_OPERATION)) {
                st.setString(1, opId);
                st.setString(2, OpType.STOP_EXECUTION.toString());
                st.setString(3, instanceId);
                st.setString(4, execId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void createExecGraphOp(String opId, String instanceId, String execId,
                                  @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Create execute graph operation in storage: { opId: {}, execId: {} }", opId, execId);
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_INSERT_OPERATION)) {
                st.setString(1, opId);
                st.setString(2, OpType.EXECUTE_GRAPH.toString());
                st.setString(3, instanceId);
                st.setString(4, execId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void deleteOp(String opId, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Delete operation from actives: { opId: {} }", opId);
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_DELETE_EXEC_OPERATION)) {
                st.setString(1, opId);
                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot delete unknown operation: { opId: {} }", opId);
                    throw new RuntimeException("Operation with id='%s' not found".formatted(opId));
                }
            }
        });
    }

    @Override
    public void deleteOps(Collection<String> opIds, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Delete operations from actives: { opIds: {} }", JsonUtils.printAsArray(opIds));
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_DELETE_EXEC_OPERATIONS)) {
                var sqlArr = connection.createArrayOf("TEXT", opIds.toArray());
                st.setArray(1, sqlArr);
                if (st.executeUpdate() < opIds.size()) {
                    LOG.error("Some operation was not deleted from actives: { opIds: {} }",
                        JsonUtils.printAsArray(opIds));
                    throw new RuntimeException("Some operation not found, list ids " + JsonUtils.printAsArray(opIds));
                }
            }
        });
    }

    @Override
    public void putState(String opId, ExecuteGraphState state, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Update execute graph operation state: { opId: {}, state: {} }", opId, state.toString());
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_UPDATE_EXECUTE_GRAPH_OP_STATE)) {
                st.setString(1, objectMapper.writeValueAsString(state));
                st.setString(2, opId);
                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot update graph execution state for unknown operation: { opId: {} }", opId);
                    throw new RuntimeException("ExecGraph operation with id='%s' not found".formatted(opId));
                }
            } catch (JsonProcessingException e) {
                var mes = "Cannot dump value of graph execution state";
                LOG.error(mes + ": {}", e.getMessage());
                throw new RuntimeException(mes, e);
            }
        });
    }

    @Override
    public List<OpInfo> listOpsInfo(String execId, @Nullable TransactionHandle transaction) throws SQLException {
        final var result = new ArrayList<OpInfo>();
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_SELECT_EXEC_OPERATIONS)) {
                st.setString(1, execId);
                var rs = st.executeQuery();
                while (rs.next()) {
                    result.add(new OpInfo(rs.getString("op_id"), OpType.valueOf(rs.getString("op_type"))));
                }
            }
        });
        return result;
    }

    @Override
    public List<String> listOpsIdsToCancel(String execId, TransactionHandle transaction) throws SQLException {
        final var result = new ArrayList<String>();
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_SELECT_EXEC_OPERATIONS)) {
                st.setString(1, execId);
                var rs = st.executeQuery();
                while (rs.next()) {
                    if (OpType.valueOf(rs.getString("op_type")) != OpType.STOP_EXECUTION) {
                        result.add(rs.getString("op_id"));
                    }
                }
            }
        });
        return result;
    }
}
