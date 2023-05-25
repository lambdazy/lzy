package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.GraphDao;
import ai.lzy.graph.model.Graph;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
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
import java.util.Collections;
import java.util.List;

@Singleton
public class GraphDaoImpl implements GraphDao {
    private static final Logger LOG = LogManager.getLogger(GraphDaoImpl.class);

    private static final String GRAPH_INSERT_FIELDS_LIST = """
        id, op_id, status, workflow_id, workflow_name, user_id, graph_description, error_description, 
        failed_task_id, failed_task_name, last_updated, owner_instance_id""";

    private static final String GRAPH_SELECT_FIELDS_LIST = """
        id, op_id, status::text as status, workflow_id, workflow_name, user_id, graph_description, 
        error_description, failed_task_id, failed_task_name, last_updated, owner_instance_id""";

    private final GraphExecutorDataSource storage;
    private final ServiceConfig config;

    @Inject
    public GraphDaoImpl(GraphExecutorDataSource storage, ServiceConfig config) {
        this.storage = storage;
        this.config = config;
    }

    @Override
    public void create(Graph graph, TransactionHandle transaction) throws SQLException {
        LOG.info("Saving graph: {}", graph);

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(
                "INSERT INTO graph (" + GRAPH_INSERT_FIELDS_LIST + ")"
                    + "VALUES (?, ?, ?::status, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
            {
                int count = 0;
                st.setString(++count, graph.id());
                st.setString(++count, graph.operationId());
                st.setString(++count, graph.status().toString());
                st.setString(++count, graph.workflowId());
                st.setString(++count, graph.workflowName());
                st.setString(++count, graph.userId());
                st.setString(++count, graph.getDescription());
                st.setString(++count, graph.errorDescription());
                st.setString(++count, graph.failedTaskId());
                st.setString(++count, graph.failedTaskName());
                st.setTimestamp(++count, Timestamp.valueOf(LocalDateTime.now()));
                st.setString(++count, config.getInstanceId());

                st.execute();
            }
        });
    }

    @Override
    public void update(Graph graph, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, connection -> {
            try (final Connection con = storage.connect();
                 final PreparedStatement st = con.prepareStatement("""
                 UPDATE graph
                 SET error_description = ?,
                     failed_task_id = ?,
                     failed_task_name = ?,
                     status = ?::status,
                     last_updated = ?
                 WHERE id = ?"""))
            {
                int count = 0;
                st.setString(++count, graph.errorDescription());
                st.setString(++count, graph.failedTaskId());
                st.setString(++count, graph.failedTaskName());
                st.setString(++count, graph.status().name());
                st.setTimestamp(++count, Timestamp.valueOf(LocalDateTime.now()));

                st.setString(++count, graph.id());
                st.executeUpdate();
            }
        });
    }

    @Override
    public Graph getById(String graphId) throws SQLException {
       try (final Connection con = storage.connect()) {
           final PreparedStatement st = con.prepareStatement("""
               SELECT %s
               FROM graph
               WHERE id = ?""".formatted(GRAPH_SELECT_FIELDS_LIST));
           st.setString(1, graphId);
           try (ResultSet s = st.executeQuery()) {
               if (!s.isBeforeFirst()) {
                   return null;
               }
               s.next();
               return fromResultSet(s);
           }
       }
    }

    @Override
    public List<Graph> getByInstance(String instanceId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement("""
                SELECT %s
                FROM graph
                WHERE owner_instance_id = ?""".formatted(GRAPH_SELECT_FIELDS_LIST));
            st.setString(1, instanceId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return new ArrayList<>();
                }
                List<Graph> list = new ArrayList<>();
                while (s.next()) {
                    list.add(fromResultSet(s));
                }
                return list;
            }
        }
    }

    private Graph fromResultSet(ResultSet resultSet) throws SQLException {
        final String graphId = resultSet.getString("id");
        final String operationId = resultSet.getString("op_id");
        final Graph.Status status = Graph.Status.valueOf(resultSet.getString("status"));
        final String workflowId = resultSet.getString("workflow_id");
        final String workflowName = resultSet.getString("workflow_name");
        final String userId = resultSet.getString("user_id");
        final String graphDescription = resultSet.getString("graph_description");
        final String errorDescription = resultSet.getString("error_description");
        final String failedTaskId = resultSet.getString("failed_task_id");
        final String failedTaskName = resultSet.getString("failed_task_name");

        return new Graph(graphId, operationId, status,
            workflowId, workflowName, userId, Collections.emptyMap(),
            errorDescription, failedTaskId, failedTaskName
        );
    }
}
