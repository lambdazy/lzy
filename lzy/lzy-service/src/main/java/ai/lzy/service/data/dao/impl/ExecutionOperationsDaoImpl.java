package ai.lzy.service.data.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.data.ExecuteGraphOperationState;
import ai.lzy.service.data.StartExecutionOperationState;
import ai.lzy.service.data.StopExecutionOperationState;
import ai.lzy.service.data.dao.ExecutionOperationsDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class ExecutionOperationsDaoImpl implements ExecutionOperationsDao {
    private static final Logger LOG = LogManager.getLogger(ExecutionOperationsDaoImpl.class);

    private static final String INSERT_OPERATION = """
        INSERT INTO execution_operations (op_id, op_type, service_instance_id, execution_id, state_json)
        VALUES (?, ?, ?, ?, ?)
        """;

    private static final String SELECT_EXEC_OPERATIONS = """
        SELECT op_id, op_type, execution_id FROM execution_operations WHERE execution_id = ?
        """;

    private final Storage storage;
    private final ObjectMapper objectMapper;

    public ExecutionOperationsDaoImpl(Storage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void create(String opId, String instanceId, String execId,
                       @Nullable StartExecutionOperationState initial,
                       @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Create start execution operation in storage: { opId: {}, execId: {} }", opId, execId);

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(INSERT_OPERATION)) {
                statement.setString(1, opId);
                statement.setString(2, OpType.START.toString());
                statement.setString(3, instanceId);
                statement.setString(4, execId);
                statement.setString(5, objectMapper.writeValueAsString(initial));
            } catch (JsonProcessingException jpe) {
                LOG.error("Cannot dump start execution operation state to storage");
                throw new SQLException(jpe);
            }
        });
    }

    @Override
    public void create(String opId, String instanceId, String execId,
                       @Nullable StopExecutionOperationState initial,
                       @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Create stop execution operation in storage: { opId: {}, execId: {} }", opId, execId);

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(INSERT_OPERATION)) {
                statement.setString(1, opId);
                statement.setString(2, OpType.STOP.toString());
                statement.setString(3, instanceId);
                statement.setString(4, execId);
                statement.setString(5, objectMapper.writeValueAsString(initial));
                statement.executeUpdate();
            } catch (JsonProcessingException jpe) {
                LOG.error("Cannot dump stop execution operation state to storage");
                throw new SQLException(jpe);
            }
        });
    }

    @Override
    public void create(String opId, String instanceId, String execId,
                       @Nullable ExecuteGraphOperationState initial,
                       @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Create execute graph operation in storage: { opId: {}, execId: {} }", opId, execId);

        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(INSERT_OPERATION)) {
                statement.setString(1, opId);
                statement.setString(2, OpType.EXECUTE_GRAPH.toString());
                statement.setString(3, instanceId);
                statement.setString(4, execId);
                statement.setString(5, objectMapper.writeValueAsString(initial));
            } catch (JsonProcessingException jpe) {
                LOG.error("Cannot dump execute graph operation state to storage");
                throw new SQLException(jpe);
            }
        });
    }

    @Override
    public List<OpInfo> get(String execId, TransactionHandle transaction) throws SQLException {
        LOG.debug("Get list execution operations: { execId: {} }", execId);

        final var result = new ArrayList<OpInfo>();
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(SELECT_EXEC_OPERATIONS)) {
                statement.setString(1, execId);
                var rs = statement.executeQuery();
                while (rs.next()) {
                    result.add(new OpInfo(rs.getString("op_id"), OpType.valueOf(rs.getString("op_type"))));
                }
            }
        });
        return result;
    }
}
