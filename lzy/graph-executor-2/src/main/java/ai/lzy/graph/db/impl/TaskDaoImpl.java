package ai.lzy.graph.db.impl;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.graph.db.TaskDao;
import ai.lzy.graph.model.TaskSlotDescription;
import ai.lzy.graph.model.TaskState;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Singleton
public class TaskDaoImpl implements TaskDao {
    private static final Logger LOG = LogManager.getLogger(TaskDaoImpl.class);

    private static final String TASK_INSERT_FIELDS_LIST = """
        id, task_name, op_id, graph_id, status, workflow_id, workflow_name, user_id,
        task_description, task_state, alloc_session, error_description, owner_instance_id""";

    private static final String TASK_SELECT_FIELDS_LIST = """
        task.id, task.task_name, task.op_id, task.graph_id, task.status::text as status, task.workflow_id,
        task.workflow_name, task.user_id, task.task_description, task.task_state, task.alloc_session,
        task.error_description, task.owner_instance_id""";

    private static final String TASK_INSERT_STATEMENT = """
        INSERT INTO task (%s) VALUES (?, ?, ?, ?, ?::task_status, ?, ?, ?, ?, ?, ?, ?, ?)"""
        .formatted(TASK_INSERT_FIELDS_LIST);

    private static final String TASK_DEPENDENCY_INSERT_STATEMENT = """
        INSERT INTO task_dependency (task_id, dependent_task_id) VALUES (?, ?)""";

    private static final String TASK_UPDATE_STATEMENT = """
        UPDATE task
        SET error_description = ?,
            status = ?::task_status,
            alloc_session = ?,
            task_state = ?
        WHERE id = ?""";

    private static final String TASK_GET_BY_ID_STATEMENT = """
        SELECT %s,
          STRING_AGG(t1.dependent_task_id, ',') as dependend_from,
          STRING_AGG(t2.task_id, ',') as dependend_on
        FROM task
          LEFT JOIN task_dependency as t1 ON task.id = t1.task_id
          LEFT JOIN task_dependency as t2 ON task.id = t2.dependent_task_id
        WHERE task.id = ?
        GROUP BY task.id""".formatted(TASK_SELECT_FIELDS_LIST);

    private static final String TASK_GET_BY_GRAPH_STATEMENT = """
        SELECT %s,
          STRING_AGG(t1.dependent_task_id, ',') as dependend_from,
          STRING_AGG(t2.task_id, ',') as dependend_on
        FROM task
          LEFT JOIN task_dependency as t1 ON task.id = t1.task_id
          LEFT JOIN task_dependency as t2 ON task.id = t2.dependent_task_id
        WHERE task.graph_id = ?
        GROUP BY task.id""".formatted(TASK_SELECT_FIELDS_LIST);

    private static final String TASK_GET_BY_INSTANCE_STATEMENT = """
        SELECT %s,
          STRING_AGG(t1.dependent_task_id, ',') as dependend_from,
          STRING_AGG(t2.task_id, ',') as dependend_on
        FROM task
          LEFT JOIN task_dependency as t1 ON task.id = t1.task_id
          LEFT JOIN task_dependency as t2 ON task.id = t2.dependent_task_id
          INNER JOIN graph ON graph.id = task.graph_id
        WHERE task.owner_instance_id = ? AND graph.status not in ('FAILED', 'COMPLETED')
        GROUP BY task.id""".formatted(TASK_SELECT_FIELDS_LIST);

    private final GraphExecutorDataSource storage;
    private final ServiceConfig config;
    private final ObjectMapper objectMapper;

    @Inject
    public TaskDaoImpl(GraphExecutorDataSource storage, ServiceConfig config) {
        this.storage = storage;
        this.config = config;
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public void createTasks(List<TaskState> tasks, TransactionHandle transaction) throws SQLException {
        LOG.debug("Saving tasks: {}", tasks.stream().map(TaskState::id).toList());

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(TASK_INSERT_STATEMENT)) {
                for (TaskState task: tasks) {
                    int count = 0;
                    st.setString(++count, task.id());
                    st.setString(++count, task.name());
                    st.setString(++count, task.operationId());
                    st.setString(++count, task.graphId());
                    st.setString(++count, task.status().toString());
                    st.setString(++count, task.executionId());
                    st.setString(++count, task.workflowName());
                    st.setString(++count, task.userId());
                    st.setString(++count, objectMapper.writeValueAsString(task.taskSlotDescription()));
                    st.setString(++count, objectMapper.writeValueAsString(task.executingState()));
                    st.setString(++count, task.allocatorSession());
                    st.setString(++count, task.errorDescription());
                    st.setString(++count, config.getInstanceId());

                    st.addBatch();
                }
                st.executeBatch();
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }

            try (PreparedStatement st = connection.prepareStatement(TASK_DEPENDENCY_INSERT_STATEMENT)) {
                for (TaskState task: tasks) {
                    for (String id: task.tasksDependedFrom()) {
                        st.setString(1, task.id());
                        st.setString(2, id);
                        st.addBatch();
                    }
                    st.executeBatch();
                }
            }
        });
    }

    @Override
    public void updateTask(TaskState task, @Nullable TransactionHandle transaction) throws SQLException {
        LOG.debug("Updating task: {}", task.id());

        DbOperation.execute(transaction, storage, connection -> {
            try (final Connection con = storage.connect();
                 final PreparedStatement st = con.prepareStatement(TASK_UPDATE_STATEMENT))
            {
                int count = 0;
                st.setString(++count, task.errorDescription());
                st.setString(++count, task.status().name());
                st.setString(++count, task.allocatorSession());
                st.setString(++count, objectMapper.writeValueAsString(task.executingState()));

                st.setString(++count, task.id());
                st.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
    }

    @Override
    @Nullable
    public TaskState getTaskById(String taskId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement(TASK_GET_BY_ID_STATEMENT);
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
    public List<TaskState> getTasksByGraph(String graphId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement(TASK_GET_BY_GRAPH_STATEMENT);
            st.setString(1, graphId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return new ArrayList<>();
                }
                List<TaskState> list = new ArrayList<>();
                while (s.next()) {
                    list.add(taskFromResultSet(s));
                }
                return list;
            }
        }
    }

    @Override
    public List<TaskState> getTasksByInstance(String instanceId) throws SQLException {
        try (final Connection con = storage.connect()) {
            final PreparedStatement st = con.prepareStatement(TASK_GET_BY_INSTANCE_STATEMENT);
            st.setString(1, instanceId);
            try (ResultSet s = st.executeQuery()) {
                if (!s.isBeforeFirst()) {
                    return new ArrayList<>();
                }
                List<TaskState> list = new ArrayList<>();
                while (s.next()) {
                    list.add(taskFromResultSet(s));
                }
                return list;
            }
        }
    }

    private TaskState taskFromResultSet(ResultSet resultSet) throws SQLException {
        final String taskId = resultSet.getString("id");
        final String taskName = resultSet.getString("task_name");
        final String opId = resultSet.getString("op_id");
        final String graphId = resultSet.getString("graph_id");
        final TaskState.Status status = TaskState.Status.valueOf(resultSet.getString("status"));
        final String workflowId = resultSet.getString("workflow_id");
        final String workflowName = resultSet.getString("workflow_name");
        final String userId = resultSet.getString("user_id");
        final String taskDescription = resultSet.getString("task_description");
        final String taskState = resultSet.getString("task_state");
        final String errorDescription = resultSet.getString("error_description");
        final String allocSession = resultSet.getString("alloc_session");
        final String dependentsFrom = resultSet.getString("dependend_from");
        final String dependentsOn = resultSet.getString("dependend_on");
        final TaskSlotDescription taskSlotDescription;
        final TaskState.ExecutingState taskOpExecutingState;

        try {
            taskSlotDescription = objectMapper.readValue(taskDescription, TaskSlotDescription.class);
            taskOpExecutingState = objectMapper.readValue(taskState, TaskState.ExecutingState.class);
        } catch (JsonProcessingException e) {
            throw new SQLException(e);
        }

        return new TaskState(taskId, taskName, opId, graphId, status, workflowId, workflowName,
            userId, errorDescription, taskSlotDescription, allocSession, taskOpExecutingState,
            parseDependents(dependentsOn), parseDependents(dependentsFrom)
        );
    }

    private List<String> parseDependents(String str) {
        return str == null ? Collections.emptyList() : Arrays.stream(str.split(",")).distinct().toList();
    }
}
