package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.Task;
import ai.lzy.graph.model.TaskOperation;
import ai.lzy.graph.services.impl.ExecuteTaskAction;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Singleton
public class TaskDaoImpl implements TaskDao {
    private static final Logger LOG = LogManager.getLogger(TaskDaoImpl.class);

    private static final String TASK_INSERT_FIELDS_LIST = """
        id, task_name, graph_id, status, workflow_id, workflow_name, user_id,
        task_description, error_description, owner_instance_id""";

    private static final String TASK_SELECT_FIELDS_LIST = """
        task.id, task.task_name, task.graph_id, task.status::text as status, task.workflow_id,
        task.workflow_name, task.user_id, task.task_description, task.error_description, task.owner_instance_id""";

    private static final String TASK_OPERATION_INSERT_FIELDS_LIST = """
        id, task_id, started_at, owner_instance_id, status, task_state, error_description""";

    private static final String TASK_OPERATION_SELECT_FIELDS_LIST = """
        id, task_id, started_at, owner_instance_id, status::text as task_op_status, task_state, error_description""";

    private final GraphExecutorDataSource storage;
    private final OperationDao operationDao;
    private final OperationsExecutor operationsExecutor;
    private final ServiceConfig config;

    @Inject
    public TaskDaoImpl(GraphExecutorDataSource storage,
                       @Named("GraphExecutorOperationDao") OperationDao operationDao,
                       @Named("GraphExecutorOperationsExecutor") OperationsExecutor operationsExecutor,
                       ServiceConfig config) {
        this.storage = storage;
        this.config = config;
        this.operationsExecutor = operationsExecutor;
        this.operationDao = operationDao;
    }

    @Override
    public void createTask(Task task, TransactionHandle transaction) throws SQLException {
        LOG.info("Saving task: {}", task);

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(
                "INSERT INTO task (" + TASK_INSERT_FIELDS_LIST + ")"
                    + "VALUES (?, ?, ?, ?::status, ?, ?, ?, ?, ?, ?)"))
            {
                int count = 0;
                st.setString(++count, task.id());
                st.setString(++count, task.name());
                st.setString(++count, task.graphId());
                st.setString(++count, task.status().toString());
                st.setString(++count, task.workflowId());
                st.setString(++count, task.workflowName());
                st.setString(++count, task.userId());
                st.setString(++count, task.getDescription());
                st.setString(++count, task.errorDescription());
                st.setString(++count, config.getInstanceId());

                st.execute();
            }
        });
    }

    @Override
    public void updateTask(Task task, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, connection -> {
            try (final Connection con = storage.connect();
                 final PreparedStatement st = con.prepareStatement("""
                 UPDATE task
                 SET error_description = ?,
                     status = ?::status
                 WHERE id = ?"""))
            {
                int count = 0;
                st.setString(++count, task.errorDescription());
                st.setString(++count, task.status().name());

                st.setString(++count, task.id());
                st.executeUpdate();
            }
        });
    }

    @Override
    public Task getTaskById(String taskId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement("""
                SELECT %s,
                  STRING_AGG(t1.dependent_task_id, ',') as dependend_from,
                  STRING_AGG(t2.task_id, ',') as dependend_on
                FROM task
                  LEFT JOIN task_dependency as t1 ON task.id = t1.task_id
                  LEFT JOIN task_dependency as t2 ON task.id = t2.dependent_task_id
                WHERE task.id = ?
                GROUP BY task.id""".formatted(TASK_SELECT_FIELDS_LIST));
            st.setString(1, taskId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return null;
                }
                s.next();
                return taskFromResultSet(s);
            }
        }
    }

    @Override
    public List<Task> getTasksByGraph(String graphId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement("""
                SELECT %s,
                  STRING_AGG(t1.dependent_task_id, ',') as dependend_from,
                  STRING_AGG(t2.task_id, ',') as dependend_on
                FROM task
                  LEFT JOIN task_dependency as t1 ON task.id = t1.task_id
                  LEFT JOIN task_dependency as t2 ON task.id = t2.dependent_task_id
                WHERE task.graph_id = ?
                GROUP BY task.id""".formatted(TASK_SELECT_FIELDS_LIST));
            st.setString(1, graphId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return new ArrayList<>();
                }
                List<Task> list = new ArrayList<>();
                while (s.next()) {
                    list.add(taskFromResultSet(s));
                }
                return list;
            }
        }
    }

    @Override
    public List<Task> getTasksByInstance(String instanceId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement("""
                SELECT %s,
                  STRING_AGG(t1.dependent_task_id, ',') as dependend_from,
                  STRING_AGG(t2.task_id, ',') as dependend_on
                FROM task
                  LEFT JOIN task_dependency as t1 ON task.id = t1.task_id
                  LEFT JOIN task_dependency as t2 ON task.id = t2.dependent_task_id
                WHERE task.owner_instance_id = ?
                GROUP BY task.id""".formatted(TASK_SELECT_FIELDS_LIST));
            st.setString(1, instanceId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return new ArrayList<>();
                }
                List<Task> list = new ArrayList<>();
                while (s.next()) {
                    list.add(taskFromResultSet(s));
                }
                return list;
            }
        }
    }

    @Override
    public void createTaskOperation(TaskOperation taskOperation, TransactionHandle transaction)
        throws SQLException
    {
        LOG.info("Saving task operation: {}", taskOperation);

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(
                "INSERT INTO task_operation (" + TASK_OPERATION_INSERT_FIELDS_LIST + ")"
                    + "VALUES (?, ?, ?, ?, ?::task_op_status, ?, ?)"))
            {
                int count = 0;
                st.setString(++count, taskOperation.id());
                st.setString(++count, taskOperation.taskId());
                st.setTimestamp(++count, Timestamp.from(taskOperation.startedAt()));
                st.setString(++count, config.getInstanceId());
                st.setString(++count, taskOperation.status().toString());
                st.setString(++count, "");
                st.setString(++count, taskOperation.errorDescription());

                st.execute();
            }
        });
    }

    @Override
    public void updateTaskOperation(TaskOperation taskOperation, TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (final Connection con = storage.connect();
                 final PreparedStatement st = con.prepareStatement("""
                 UPDATE task_operation
                 SET error_description = ?,
                     status = ?::status,
                     task_state = ?
                 WHERE id = ?"""))
            {
                int count = 0;
                st.setString(++count, taskOperation.errorDescription());
                st.setString(++count, taskOperation.status().name());
                st.setString(++count, "");

                st.setString(++count, taskOperation.id());
                st.executeUpdate();
            }
        });
    }

    @Override
    public TaskOperation getTaskOperationById(String taskOperationId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement("""
                SELECT %s
                FROM task_operation
                WHERE id = ?
                """.formatted(TASK_OPERATION_SELECT_FIELDS_LIST));
            st.setString(1, taskOperationId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return null;
                }
                s.next();
                return taskOperationFromResultSet(s);
            }
        }
    }

    @Override
    public List<TaskOperation> getTasksOperationsByInstance(String instanceId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement("""
                SELECT %s
                FROM task_operation
                WHERE owner_instance_id = ?
                """.formatted(TASK_OPERATION_SELECT_FIELDS_LIST));
            st.setString(1, instanceId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return new ArrayList<>();
                }
                List<TaskOperation> list = new ArrayList<>();
                while (s.next()) {
                    list.add(taskOperationFromResultSet(s));
                }
                return list;
            }
        }
    }

    private Task taskFromResultSet(ResultSet resultSet) throws SQLException {
        final String taskId = resultSet.getString("id");
        final String taskName = resultSet.getString("task_name");
        final String graphId = resultSet.getString("graph_id");
        final Task.Status status = Task.Status.valueOf(resultSet.getString("status"));
        final String workflowId = resultSet.getString("workflow_id");
        final String workflowName = resultSet.getString("workflow_name");
        final String userId = resultSet.getString("user_id");
        final String taskDescription = resultSet.getString("task_description");
        final String errorDescription = resultSet.getString("error_description");
        final String dependentsFrom = resultSet.getString("dependend_from");
        final String dependentsOn = resultSet.getString("dependend_on");

        return new Task(taskId, taskName, graphId, status,
            workflowId, workflowName, userId, errorDescription, null, Collections.emptyMap(),
            Arrays.stream(dependentsOn.split(",")).distinct().toList(),
            Arrays.stream(dependentsFrom.split(",")).distinct().toList()
        );
    }

    private TaskOperation taskOperationFromResultSet(ResultSet resultSet) throws SQLException {
        final String taskOperationId = resultSet.getString("id");
        final String taskId = resultSet.getString("task_id");
        final Timestamp startedAt = resultSet.getTimestamp("started_at");
        final TaskOperation.Status status = TaskOperation.Status.valueOf(resultSet.getString("status"));
        final String taskState = resultSet.getString("task_state");
        final String errorDescription = resultSet.getString("error_description");

        return new TaskOperation(taskOperationId, taskId, startedAt.toInstant(), status, errorDescription,
            new ExecuteTaskAction(taskId, taskState, storage, operationDao, operationsExecutor));
    }
}
