package ru.yandex.cloud.ml.platform.lzy.graph.db.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
        "workflow_id, id, "
        + "error_description, status, "
        + "graph_description_json, task_executions_json, "
        + "current_execution_group_json, last_updated, acquired_before ";

    @Inject
    public GraphExecutionDaoImpl(Storage storage) {
        this.storage = storage;
    }

    @Override
    public GraphExecutionState create(String workflowId, GraphDescription description) throws GraphDaoException {
        try (final Connection con = storage.connect();
             final PreparedStatement st = con.prepareStatement(
                 "INSERT INTO graph_execution_state ( "
                     + GRAPH_FIELDS_LIST
                     + " ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);")
        ) {
            String id = UUID.randomUUID().toString();
            GraphExecutionState state = GraphExecutionState.builder()
                .withWorkflowId(workflowId)
                .withId(id)
                .withDescription(description)
                .build();
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
            throw new GraphDaoException(e);
        }
    }

    @Override
    public List<GraphExecutionState> filter(GraphExecutionState.Status status) throws GraphDaoException {
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
            throw new GraphDaoException(e);
        }
    }

    @Override
    public List<GraphExecutionState> list(String workflowId) throws GraphDaoException {
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
            throw new GraphDaoException(e);
        }
    }

    @Nullable
    @Override
    public GraphExecutionState acquire(String workflowId, String graphExecutionId,
                                       long upTo, TemporalUnit unit) throws GraphDaoException {
        final AtomicReference<GraphExecutionState> state = new AtomicReference<>();
        executeInTransaction(conn -> {
            try (final PreparedStatement st = conn.prepareStatement(
                "SELECT "
                    + GRAPH_FIELDS_LIST
                    + " FROM graph_execution_state WHERE workflow_id = ? AND id = ?")
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
                    final Timestamp timestamp = s.getTimestamp("acquired_before");
                    if (timestamp != null && timestamp.after(Timestamp.valueOf(LocalDateTime.now()))) {
                        throw new GraphDaoException(
                            String.format("Cannot acquire graph <%s> in workflow <%s>", graphExecutionId, workflowId)
                        );
                    }
                }
            }

            try (final PreparedStatement st = conn.prepareStatement(
                    """
                     UPDATE graph_execution_state
                     SET acquired_before = ?
                     WHERE workflow_id = ? AND id = ?""")
            ) {
                st.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now().plus(upTo, unit)));
                st.setString(2, workflowId);
                st.setString(3, graphExecutionId);
                st.executeUpdate();
            }

        });
        return state.get();
    }

    @Override
    public void free(GraphExecutionState graph) throws GraphDaoException {
        try (final Connection con = storage.connect();
            final PreparedStatement st = con.prepareStatement(
                """
                    UPDATE graph_execution_state
                     SET workflow_id = ?,
                     id = ?,
                     error_description = ?,
                     status = ?,
                     graph_description_json = ?,
                     task_executions_json = ?,
                     current_execution_group_json = ?, last_updated = ?,
                     acquired_before = ?
                     WHERE workflow_id = ? AND id = ?;""")
        ) {
            setGraphFields(st, graph);
            st.setString(10, graph.workflowId());
            st.setString(11, graph.id());
            st.executeUpdate();
        } catch (Exception e) {
            throw new GraphDaoException(e);
        }
    }

    private interface Transaction {
        void execute(Connection connection) throws Exception;
    }

    private void executeInTransaction(Transaction transaction) throws GraphDaoException {
        try (final Connection con = storage.connect()) {
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
        st.setTimestamp(9, null);
    }
}
