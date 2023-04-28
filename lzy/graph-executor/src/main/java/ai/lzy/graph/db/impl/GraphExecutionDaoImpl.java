package ai.lzy.graph.db.impl;

import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.TaskExecution;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Transaction;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.DaoException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
@Requires(notEnv = "test-mock")
public class GraphExecutionDaoImpl implements GraphExecutionDao {
    private static final Logger LOG = LogManager.getLogger(GraphExecutionDaoImpl.class);

    private final GraphExecutorDataSource storage;
    private final ObjectMapper objectMapper;

    private static final String GRAPH_INSERT_FIELDS_LIST = """
        workflow_id, workflow_name, user_id, id, error_description, failed_task_id, failed_task_name, status,
        graph_description_json, task_executions_json, current_execution_group_json, last_updated, acquired""";

    private static final String GRAPH_SELECT_FIELDS_LIST = """
        workflow_id, workflow_name, user_id, id, error_description, failed_task_id, failed_task_name,
        status::text as status, graph_description_json, task_executions_json, current_execution_group_json,
        last_updated, acquired""";

    @Inject
    public GraphExecutionDaoImpl(GraphExecutorDataSource storage) {
        this.storage = storage;
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public GraphExecutionState create(String executionId, String workflowName, String userId,
                                      GraphDescription description, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Save graph execution state: { executionId: {}, workflowName: {}, userId: {}, desc: {} }",
            executionId, workflowName, userId, description);

        GraphExecutionState[] state = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(
                "INSERT INTO graph_execution_state (" + GRAPH_INSERT_FIELDS_LIST + ")"
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?::graph_execution_status, ?, ?, ?, ?, ?)"))
            {
                String id = UUID.randomUUID().toString();
                state[0] = GraphExecutionState.builder()
                    .withWorkflowId(executionId)
                    .withWorkflowName(workflowName)
                    .withUserId(userId)
                    .withId(id)
                    .withDescription(description)
                    .build();
                setGraphFields(st, state[0]);
                st.execute();
            } catch (JsonProcessingException e) {
                throw new SQLException("Cannot dump values for graph fields", e);
            }
        });

        return state[0];
    }

    @Nullable
    @Override
    public GraphExecutionState get(String workflowId, String graphExecutionId) throws DaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement("""
                 SELECT %s
                 FROM graph_execution_state
                 WHERE workflow_id = ? AND id = ?""".formatted(GRAPH_SELECT_FIELDS_LIST)))
        {
            st.setString(1, workflowId);
            st.setString(2, graphExecutionId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return null;
                }
                s.next();
                return fromResultSet(s);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<GraphExecutionState> filter(GraphExecutionState.Status status) throws DaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement("""
                 SELECT %s
                 FROM graph_execution_state
                 WHERE status = ?::graph_execution_status
                 ORDER BY last_updated""".formatted(GRAPH_SELECT_FIELDS_LIST)))
        {
            st.setString(1, status.name());
            try (ResultSet s = st.executeQuery()) {
                return readStateList(s);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<GraphExecutionState> list(String workflowId) throws DaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement("""
                 SELECT %s
                 FROM graph_execution_state
                 WHERE workflow_id = ?""".formatted(GRAPH_SELECT_FIELDS_LIST)))
        {
            st.setString(1, workflowId);
            try (ResultSet s = st.executeQuery()) {
                return readStateList(s);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Nullable
    @Override
    public GraphExecutionState acquire(String workflowId, String graphExecutionId) throws DaoException {
        final AtomicReference<GraphExecutionState> state = new AtomicReference<>();
        Transaction.execute(storage, conn -> {
            try (final PreparedStatement st = conn.prepareStatement("""
                SELECT %s
                FROM graph_execution_state WHERE workflow_id = ? AND id = ?
                FOR UPDATE""".formatted(GRAPH_SELECT_FIELDS_LIST)))
            {
                st.setString(1, workflowId);
                st.setString(2, graphExecutionId);
                try (ResultSet s = st.executeQuery()) {
                    if (!s.isBeforeFirst()) {
                        state.set(null);
                        return true;
                    }
                    s.next();
                    state.set(fromResultSet(s));
                    final boolean acquired = s.getBoolean("acquired");
                    if (acquired) {
                        throw new DaoException(
                            String.format("Cannot acquire graph <%s> in workflow <%s>", graphExecutionId, workflowId)
                        );
                    }
                }
            }

            try (final PreparedStatement st = conn.prepareStatement("""
                UPDATE graph_execution_state
                SET acquired = ?
                WHERE workflow_id = ? AND id = ?"""))
            {
                st.setBoolean(1, true);
                st.setString(2, workflowId);
                st.setString(3, graphExecutionId);
                st.executeUpdate();
            }

            return true;
        });
        return state.get();
    }

    @Override
    public void updateAndFree(GraphExecutionState graph) throws DaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement("""
                 UPDATE graph_execution_state
                 SET error_description = ?,
                     failed_task_id = ?,
                     failed_task_name = ?,
                     status = ?::graph_execution_status,
                     graph_description_json = ?,
                     task_executions_json = ?,
                     current_execution_group_json = ?,
                     last_updated = ?,
                     acquired = ?
                 WHERE workflow_id = ? AND id = ?"""))
        {
            int count = 0;
            st.setString(++count, graph.errorDescription());
            st.setString(++count, graph.failedTaskId());
            st.setString(++count, graph.failedTaskName());
            st.setString(++count, graph.status().name());
            st.setString(++count, objectMapper.writeValueAsString(graph.description()));
            st.setString(++count, objectMapper.writeValueAsString(graph.executions()));
            st.setString(++count, objectMapper.writeValueAsString(graph.currentExecutionGroup()));
            st.setTimestamp(++count, Timestamp.valueOf(LocalDateTime.now()));
            st.setBoolean(++count, false);

            st.setString(++count, graph.workflowId());
            st.setString(++count, graph.id());
            st.executeUpdate();
        } catch (Exception e) {
            throw new DaoException(e);
        }
    }

    private GraphExecutionState fromResultSet(ResultSet resultSet) throws SQLException, JsonProcessingException {
        final String workflowId = resultSet.getString("workflow_id");
        final String workflowName = resultSet.getString("workflow_name");
        final String userId = resultSet.getString("user_id");
        final String id = resultSet.getString("id");
        final String errorDescription = resultSet.getString("error_description");
        final String failedTaskId = resultSet.getString("failed_task_id");
        final String failedTaskName = resultSet.getString("failed_task_name");
        final GraphExecutionState.Status status = GraphExecutionState.Status.valueOf(resultSet.getString("status"));
        final String graphDescriptionJson = resultSet.getString("graph_description_json");
        final String taskExecutionsJson = resultSet.getString("task_executions_json");
        final String currentExecutionGroupJson = resultSet.getString("current_execution_group_json");
        final GraphDescription graph = objectMapper.readValue(graphDescriptionJson, GraphDescription.class);
        final List<TaskExecution> executions = objectMapper.readValue(taskExecutionsJson, new TypeReference<>() {});
        final List<TaskExecution> currentExecutionGroup = objectMapper.readValue(
            currentExecutionGroupJson, new TypeReference<>() {});
        return new GraphExecutionState(
            workflowId, workflowName, userId, id, graph, executions,
            currentExecutionGroup, status, errorDescription, failedTaskId, failedTaskName
        );
    }

    private List<GraphExecutionState> readStateList(ResultSet s) throws SQLException, JsonProcessingException {
        if (!s.isBeforeFirst()) {
            return new ArrayList<>();
        }
        List<GraphExecutionState> list = new ArrayList<>();
        while (s.next()) {
            list.add(fromResultSet(s));
        }
        return list;
    }

    private void setGraphFields(PreparedStatement st, GraphExecutionState state)
        throws SQLException, JsonProcessingException
    {
        int count = 0;
        st.setString(++count, state.workflowId());
        st.setString(++count, state.workflowName());
        st.setString(++count, state.userId());
        st.setString(++count, state.id());
        st.setString(++count, state.errorDescription());
        st.setString(++count, state.failedTaskId());
        st.setString(++count, state.failedTaskName());
        st.setString(++count, state.status().name());
        st.setString(++count, objectMapper.writeValueAsString(state.description()));
        st.setString(++count, objectMapper.writeValueAsString(state.executions()));
        st.setString(++count, objectMapper.writeValueAsString(state.currentExecutionGroup()));
        st.setTimestamp(++count, Timestamp.valueOf(LocalDateTime.now()));
        st.setBoolean(++count, false);
    }
}
