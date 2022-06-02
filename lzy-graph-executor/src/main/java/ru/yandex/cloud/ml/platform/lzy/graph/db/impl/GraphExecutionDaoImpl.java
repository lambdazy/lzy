package ru.yandex.cloud.ml.platform.lzy.graph.db.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Array;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import ru.yandex.cloud.ml.platform.lzy.graph.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph.db.Storage;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskExecution;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Singleton
public class GraphExecutionDaoImpl implements GraphExecutionDao {
    private final Storage storage;

    private static final String GRAPH_FIELDS_LIST =
        "(workflow_id, id, "
        + "error_description, status, "
        + "graph_description_json, task_executions_json, "
        + "current_execution_group_json, last_updated) ";

    @Inject
    public GraphExecutionDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public GraphExecutionState create(String workflowId, GraphDescription description) throws GraphDaoException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (final PreparedStatement st = storage.connect().prepareStatement(
            "INSERT INTO graph_execution_state "
                + GRAPH_FIELDS_LIST
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
        )) {
            String id = UUID.randomUUID().toString();
            GraphExecutionState state = new GraphExecutionState(workflowId, id, description);
            setGraphFields(st, state);
            st.execute();
            return state;

        } catch (SQLException | JsonProcessingException e) {
            throw new GraphDaoException(e);
        }
    }

    @Nullable
    @Override
    public GraphExecutionState get(String workflowId, String graphExecutionId) throws GraphDaoException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
            "SELECT " + GRAPH_FIELDS_LIST + " FROM graph_execution_state WHERE workflow_id = ? AND id = ?"
        )) {
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
            throw new GraphDaoException(e);
        }
    }

    @Override
    public List<GraphExecutionState> filter(GraphExecutionState.Status status) throws GraphDaoException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
            "SELECT " + GRAPH_FIELDS_LIST + " FROM graph_execution_state WHERE status = ?"
        )) {
            st.setString(1, status.name());
            try (ResultSet s = st.executeQuery()) {
                return readStateList(s);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new GraphDaoException(e);
        }
    }

    @Override
    public List<GraphExecutionState> list(String workflowId) throws GraphDaoException {
        try (final PreparedStatement st = storage.connect().prepareStatement(
            "SELECT " + GRAPH_FIELDS_LIST + " FROM graph_execution_state WHERE workflow_id = ?"
        )) {
            st.setString(1, workflowId);
            try (ResultSet s = st.executeQuery()) {
                return readStateList(s);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new GraphDaoException(e);
        }
    }

    @Override
    public void updateAtomic(String workflowId, String graphExecutionId, Mapper mapper) throws GraphDaoException {
        executeInTransaction(con -> {
            final GraphExecutionState state;
            try (final PreparedStatement st = con.prepareStatement(
                "SELECT " + GRAPH_FIELDS_LIST + " from graph_execution_state "
                    + "WHERE workflow_id = ? AND id = ? FOR UPDATE;")) {
                st.setString(1, workflowId);
                st.setString(2, graphExecutionId);
                try (ResultSet s = st.executeQuery()) {
                    if (!s.isBeforeFirst()) {
                        mapper.update(null); // Notify mapper about NotFound
                        return;
                    }
                    s.next();
                    state = fromResultSet(s);
                }
            }
            final GraphExecutionState graph = mapper.update(state);
            update(con, graph);
        });
    }

    @Override
    public void updateAtomic(Set<GraphExecutionState.Status> statuses, Mapper mapper) throws GraphDaoException {
        executeInTransaction(con -> {
            final GraphExecutionState state;
            try (final PreparedStatement st = con.prepareStatement(
                "SELECT " + GRAPH_FIELDS_LIST + " from graph_execution_state "
                    + "WHERE status IN ? "
                    + "ORDER BY last_updated "
                    + "LIMIT 1 "
                    + "FOR UPDATE SKIP LOCKED;"
            )) {
                Array array = con.createArrayOf(
                    "varchar",
                    statuses.stream()
                        .map(Enum::name)
                        .toArray()
                );
                st.setArray(1, array);
                try (ResultSet s = st.executeQuery()) {
                    if (!s.isBeforeFirst()) {
                        mapper.update(null);
                        return;
                    }
                    s.next();
                    state = fromResultSet(s);
                }
            }
            final GraphExecutionState graph = mapper.update(state);
            update(con, graph);
        });
    }

    private interface Transaction {
        void execute(Connection connection) throws Exception;
    }

    private void executeInTransaction(Transaction transaction) throws GraphDaoException {
        try {
            final Connection con = storage.connect();
            try {
                con.setAutoCommit(false); // To execute many queries in one transaction
                transaction.execute(con);
                con.commit();
            } catch (Exception e) {
                con.rollback();
                throw new GraphDaoException(e);
            } finally {
                con.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new GraphDaoException(e);
        }
    }

    private void update(Connection con, GraphExecutionState state) throws SQLException, JsonProcessingException {
        try (final PreparedStatement st = con.prepareStatement(
            "UPDATE graph_execution_state "
                + "SET workflow_id = ?, "
                + "id = ?, "
                + "error_description = ?, "
                + "status = ?, "
                + "graph_description_json = ?, "
                + "task_executions_json = ?, "
                + "current_execution_group_json = ?, last_updated = ? "
                + "WHERE workflow_id = ? AND id = ?;")) {
            setGraphFields(st, state);
            st.setString(9, state.workflowId());
            st.setString(10, state.id());
            st.executeUpdate();
        }
    }

    private GraphExecutionState fromResultSet(ResultSet resultSet) throws SQLException, JsonProcessingException {
        final String workflowId = resultSet.getString("workflow_id");
        final String id = resultSet.getString("id");
        final String errorDescription = resultSet.getString("error_description");
        final GraphExecutionState.Status status = GraphExecutionState.Status.valueOf(resultSet.getString("status"));
        final String graphDescriptionJson  = resultSet.getString("graph_description_json");
        final String taskExecutionsJson  = resultSet.getString("task_executions_json");
        final String currentExecutionGroupJson  = resultSet.getString("current_execution_group_json");
        final ObjectMapper objectMapper = new ObjectMapper();
        final GraphDescription graph = objectMapper.readValue(graphDescriptionJson, GraphDescription.class);
        final List<TaskExecution> executions = objectMapper.readValue(taskExecutionsJson, new TypeReference<>() {});
        final List<TaskExecution> currentExecutionGroup = objectMapper.readValue(
            currentExecutionGroupJson, new TypeReference<>() {});
        return new GraphExecutionState(
            workflowId, id, graph, executions,
            currentExecutionGroup, status, errorDescription
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
        throws SQLException, JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        st.setString(1, state.workflowId());
        st.setString(2, state.id());
        st.setString(3, state.errorDescription());
        st.setString(4, state.status().name());
        st.setString(5, objectMapper.writeValueAsString(state.description()));
        st.setString(6, objectMapper.writeValueAsString(state.executions()));
        st.setString(7, objectMapper.writeValueAsString(state.currentExecutionGroup()));
        st.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
    }
}
