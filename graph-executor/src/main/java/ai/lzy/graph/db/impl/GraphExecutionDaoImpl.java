package ai.lzy.graph.db.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Nullable;
import ai.lzy.graph.db.DaoException;
import ai.lzy.graph.db.GraphExecutionDao;
import ai.lzy.graph.db.Storage;
import ai.lzy.graph.db.Utils;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.GraphExecutionState;
import ai.lzy.graph.model.TaskExecution;

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
        "workflow_id, workflow_name, id, "
        + "error_description, status, "
        + "graph_description_json, task_executions_json, "
        + "current_execution_group_json, last_updated, acquired ";

    @Inject
    public GraphExecutionDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public GraphExecutionState create(String workflowId, String workflowName, GraphDescription description) throws DaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement(
                 "INSERT INTO graph_execution_state ( "
                     + GRAPH_FIELDS_LIST
                     + " ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
        ) {
            String id = UUID.randomUUID().toString();
            GraphExecutionState state = GraphExecutionState.builder()
                .withWorkflowId(workflowId)
                .withWorkflowName(workflowName)
                .withId(id)
                .withDescription(description)
                .build();
            setGraphFields(st, state);
            st.execute();
            return state;

        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Nullable
    @Override
    public GraphExecutionState get(String workflowId, String graphExecutionId) throws DaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement(
                 "SELECT "
                     + GRAPH_FIELDS_LIST
                     + " FROM graph_execution_state WHERE workflow_id = ? AND id = ?")
        ) {
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
             final PreparedStatement st = con.prepareStatement(
                 "SELECT " + GRAPH_FIELDS_LIST + " FROM graph_execution_state"
                     + " WHERE status = ? "
                     + " ORDER BY last_updated ")
        ) {
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
             final PreparedStatement st = con.prepareStatement(
                 "SELECT "
                     + GRAPH_FIELDS_LIST
                     + " FROM graph_execution_state WHERE workflow_id = ?")
        ) {
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
        Utils.executeInTransaction(storage, conn -> {
            try (final PreparedStatement st = conn.prepareStatement(
                "SELECT "
                    + GRAPH_FIELDS_LIST
                    + " FROM graph_execution_state WHERE workflow_id = ? AND id = ?"
                    + " FOR UPDATE")
            ) {
                st.setString(1, workflowId);
                st.setString(2, graphExecutionId);
                try (ResultSet s = st.executeQuery()) {
                    if (!s.isBeforeFirst()) {
                        state.set(null);
                        return;
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

            try (final PreparedStatement st = conn.prepareStatement(
                    """
                     UPDATE graph_execution_state
                     SET acquired = ?
                     WHERE workflow_id = ? AND id = ?""")
            ) {
                st.setBoolean(1, true);
                st.setString(2, workflowId);
                st.setString(3, graphExecutionId);
                st.executeUpdate();
            }

        });
        return state.get();
    }

    @Override
    public void free(GraphExecutionState graph) throws DaoException {
        try (final Connection con = storage.connect();
            final PreparedStatement st = con.prepareStatement(
                """
                    UPDATE graph_execution_state
                     SET workflow_id = ?,
                     workflow_name = ?,
                     id = ?,
                     error_description = ?,
                     status = ?,
                     graph_description_json = ?,
                     task_executions_json = ?,
                     current_execution_group_json = ?, last_updated = ?,
                     acquired = ?
                     WHERE workflow_id = ? AND id = ?;""")
        ) {
            setGraphFields(st, graph);
            st.setString(11, graph.workflowId());
            st.setString(12, graph.id());
            st.executeUpdate();
        } catch (Exception e) {
            throw new DaoException(e);
        }
    }

    private GraphExecutionState fromResultSet(ResultSet resultSet) throws SQLException, JsonProcessingException {
        final String workflowId = resultSet.getString("workflow_id");
        final String workflowName = resultSet.getString("workflow_name");
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
            workflowId, workflowName, id, graph, executions,
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
        st.setString(2, state.workflowName());
        st.setString(3, state.id());
        st.setString(4, state.errorDescription());
        st.setString(5, state.status().name());
        st.setString(6, objectMapper.writeValueAsString(state.description()));
        st.setString(7, objectMapper.writeValueAsString(state.executions()));
        st.setString(8, objectMapper.writeValueAsString(state.currentExecutionGroup()));
        st.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
        st.setBoolean(10, false);
    }
}
