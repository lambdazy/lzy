package ru.yandex.cloud.ml.platform.lzy.graph_executor.db.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import org.jetbrains.annotations.Nullable;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.GraphExecutionDao;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.db.Storage;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.TaskExecution;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class GraphExecutionDaoImpl implements GraphExecutionDao {
    private final Storage storage;

    @Inject
    public GraphExecutionDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public GraphExecutionState create(String workflowId, GraphDescription description) throws GraphDaoException{
        ObjectMapper objectMapper = new ObjectMapper();
        try (final PreparedStatement st = storage.connect().prepareStatement(
            "INSERT INTO graph_execution_state( " +
                "workflow_id, id, " +
                "error_description, status, " +
                "graph_description_json, task_executions_json, " +
                "current_execution_group_json" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?);"
        )) {
            String id = UUID.randomUUID().toString();
            GraphExecutionState state = new GraphExecutionState(workflowId, id, description);
            st.setString(1, state.workflowId());
            st.setString(2, state.id());
            st.setString(3, state.errorDescription());
            st.setString(4, state.status().name());
            st.setString(5, objectMapper.writeValueAsString(state.description()));
            st.setString(6, objectMapper.writeValueAsString(state.executions()));
            st.setString(7, objectMapper.writeValueAsString(state.currentExecutionGroup()));
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
            "SELECT * FROM graph_execution_state WHERE workflow_id = ? AND id = ?"
        )) {
            st.setString(1, workflowId);
            st.setString(2, graphExecutionId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()){
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
            "SELECT * FROM graph_execution_state WHERE status = ?"
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
            "SELECT * FROM graph_execution_state WHERE workflow_id = ?"
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
    public void updateAtomic(String workflowId, String graphExecutionId, Updater mapper)
        throws GraphDaoException
    {
        final Connection con;
        try {
             con = storage.connect();
        } catch (SQLException e) {
            throw new GraphDaoException(e);
        }
        try {
            con.setAutoCommit(false);
            final GraphExecutionState state;
            try (final PreparedStatement st = con.prepareStatement(
                    "SELECT * from graph_execution_state " +
                    "WHERE workflow_id = ? AND id = ? FOR UPDATE;")) {
                st.setString(1, workflowId);
                st.setString(2, graphExecutionId);
                try (ResultSet s = st.executeQuery()) {
                    if (!s.isBeforeFirst()) {
                        throw new GraphDaoException("Cannot find graph execution with id <" +
                            graphExecutionId + "> in workflow <" + workflowId +">");
                    }
                    s.next();
                    state = fromResultSet(s);
                }
            }
            final GraphExecutionState graph = mapper.update(state);
            update(con, graph);
        } catch (Exception e) {
            throw new GraphDaoException(e);
        } finally {
            try {
                con.setAutoCommit(true);
            } catch (SQLException e) {
                //noinspection ThrowFromFinallyBlock
                throw new GraphDaoException(e);
            }
        }

    }

    private void update(Connection con, GraphExecutionState s) throws SQLException, JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (final PreparedStatement st = con.prepareStatement(
            "UPDATE graph_execution_state " +
                "SET workflow_id = ?, " +
                "id = ?, " +
                "error_description = ?, " +
                "status = ?, " +
                "graph_description_json = ?, " +
                "task_executions_json = ?, " +
                "current_execution_group_json = ? " +
                "WHERE workflow_id = ? AND id = ?;")) {
            st.setString(1, s.workflowId());
            st.setString(2, s.id());
            st.setString(3, s.errorDescription());
            st.setString(4, s.status().name());
            st.setString(5, objectMapper.writeValueAsString(s.description()));
            st.setString(6, objectMapper.writeValueAsString(s.executions()));
            st.setString(7, objectMapper.writeValueAsString(s.currentExecutionGroup()));
            st.setString(8, s.workflowId());
            st.setString(9, s.id());
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
        return new GraphExecutionState(workflowId, id, graph, executions, currentExecutionGroup, status, errorDescription);
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
}
