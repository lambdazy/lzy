package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.model.GraphState;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

@Singleton
public class GraphDaoImpl implements GraphDao {
    private static final Logger LOG = LogManager.getLogger(GraphDaoImpl.class);

    private static final String GRAPH_INSERT_FIELDS_LIST = """
        id, op_id, status, workflow_id, workflow_name, user_id, allocator_session_id,
        error_description, failed_task_id, failed_task_name, last_updated, owner_instance_id""";

    private static final String GRAPH_SELECT_FIELDS_LIST = """
        id, op_id, status::text as status, workflow_id, workflow_name, user_id, allocator_session_id,
        error_description, failed_task_id, failed_task_name, last_updated, owner_instance_id""";

    private static final String GRAPH_INSERT_STATEMENT = """
        INSERT INTO graph (%s)
        VALUES (?, ?, ?::status, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".formatted(GRAPH_INSERT_FIELDS_LIST);

    private static final String GRAPH_UPDATE_STATEMENT = """
        UPDATE graph
        SET error_description = ?,
            failed_task_id = ?,
            failed_task_name = ?,
            status = ?::status,
            last_updated = ?
        WHERE id = ?""";

    private static final String GRAPH_GET_BY_ID_STATEMENT = """
        SELECT %s
        FROM graph
        WHERE id = ?""".formatted(GRAPH_SELECT_FIELDS_LIST);

    private static final String GRAPH_GET_BY_INSTANCE_STATEMENT = """
        SELECT %s
        FROM graph
        WHERE owner_instance_id = ? AND status NOT IN ('FAILED', 'COMPLETED')""".formatted(GRAPH_SELECT_FIELDS_LIST);

    private final GraphExecutorDataSource storage;
    private final ServiceConfig config;

    @Inject
    public GraphDaoImpl(GraphExecutorDataSource storage, ServiceConfig config) {
        this.storage = storage;
        this.config = config;
    }

    @Override
    public void create(GraphState graph, TransactionHandle transaction) throws SQLException {
        LOG.debug("Saving graph: {}", graph.id());

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(GRAPH_INSERT_STATEMENT)) {
                int idx = 0;
                st.setString(++idx, graph.id());
                st.setString(++idx, graph.operationId());
                st.setString(++idx, graph.status().toString());
                st.setString(++idx, graph.executionId());
                st.setString(++idx, graph.workflowName());
                st.setString(++idx, graph.userId());
                st.setString(++idx, graph.allocatorSessionId());
                st.setString(++idx, graph.errorDescription());
                st.setString(++idx, graph.failedTaskId());
                st.setString(++idx, graph.failedTaskName());
                st.setTimestamp(++idx, Timestamp.from(Instant.now()));
                st.setString(++idx, config.getInstanceId());

                st.execute();
            }
        });
    }

    @Override
    public void update(GraphState graph, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Updating graph: {}", graph.id());

        DbOperation.execute(transaction, storage, connection -> {
            try (final PreparedStatement st = connection.prepareStatement(GRAPH_UPDATE_STATEMENT)) {
                int idx = 0;
                st.setString(++idx, graph.errorDescription());
                st.setString(++idx, graph.failedTaskId());
                st.setString(++idx, graph.failedTaskName());
                st.setString(++idx, graph.status().name());
                st.setTimestamp(++idx, Timestamp.from(Instant.now()));
                st.setString(++idx, graph.id());

                st.executeUpdate();
            }
        });
    }

    @Override
    @Nullable
    public GraphState getById(String graphId) throws SQLException {
        try (var connection = storage.connect();
             PreparedStatement st = connection.prepareStatement(GRAPH_GET_BY_ID_STATEMENT))
        {
            st.setString(1, graphId);

            var rs = st.executeQuery();
            if (rs.next()) {
                return fromResultSet(rs);
            }

            return null;
        }
    }

    @Override
    public List<GraphState> getActiveByInstance(String instanceId) throws SQLException {
        try (var connection = storage.connect();
             PreparedStatement st = connection.prepareStatement(GRAPH_GET_BY_INSTANCE_STATEMENT))
        {
            st.setString(1, instanceId);

            var rs = st.executeQuery();
            var list = new ArrayList<GraphState>();
            while (rs.next()) {
                list.add(fromResultSet(rs));
            }
            return list;
        }
    }

    private GraphState fromResultSet(ResultSet resultSet) throws SQLException {
        final String graphId = resultSet.getString("id");
        final String operationId = resultSet.getString("op_id");
        final GraphState.Status status = GraphState.Status.valueOf(resultSet.getString("status"));
        final String workflowId = resultSet.getString("workflow_id");
        final String workflowName = resultSet.getString("workflow_name");
        final String userId = resultSet.getString("user_id");
        final String allocatorSessionId = resultSet.getString("allocator_session_id");
        final String errorDescription = resultSet.getString("error_description");
        final String failedTaskId = resultSet.getString("failed_task_id");
        final String failedTaskName = resultSet.getString("failed_task_name");

        return new GraphState(graphId, operationId, status, workflowId, workflowName, userId, allocatorSessionId,
            new EnumMap<>(GraphState.Status.class), errorDescription, failedTaskId, failedTaskName);
    }
}
