package ai.lzy.scheduler.db.impl;

import ai.lzy.model.db.DaoException;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.model.basic.TaskDesc;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.task.Task;
import ai.lzy.scheduler.task.TaskImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Singleton
public class TaskDaoImpl implements TaskDao {

    private static final String FIELDS = """
        id, workflow_id, workflow_name, task_description_json, status,
        rc, error_description, servant_id""";

    private final SchedulerDataSource storage;

    public TaskDaoImpl(SchedulerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Task create(String workflowId, String workflowName, TaskDesc taskDesc) throws DaoException {
        try (var conn = storage.connect(); var st = conn.prepareStatement("""
                INSERT INTO task(id, workflow_id, workflow_name, task_description_json, status)
                VALUES (?, ?, ?, ?, CAST(? AS task_status))
                """)) {
            int paramCount = 0;
            String id = UUID.randomUUID().toString();
            st.setString(++paramCount, id);
            st.setString(++paramCount, workflowId);
            st.setString(++paramCount, workflowName);

            ObjectMapper objectMapper = new ObjectMapper();
            st.setString(++paramCount, objectMapper.writeValueAsString(taskDesc));

            st.setString(++paramCount, TaskState.Status.QUEUE.toString());
            st.execute();
            return new TaskImpl(
                new TaskState(id, workflowId, workflowName, taskDesc, TaskState.Status.QUEUE, null, null, null), this);
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Nullable
    @Override
    public Task get(String taskId) throws DaoException {
        try (var conn = storage.connect(); var st = conn.prepareStatement(
                "SELECT " + FIELDS + " FROM task WHERE id = ?")) {
            int paramCount = 0;
            st.setString(++paramCount, taskId);
            try (var rs = st.executeQuery()) {
                if (!rs.isBeforeFirst()) {
                    return null;
                }
                rs.next();
                return taskFromResultSet(rs);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<Task> filter(TaskState.Status status) throws DaoException {
        try (var conn = storage.connect(); var st = conn.prepareStatement(
                "SELECT " + FIELDS + "  FROM task WHERE status = CAST(? AS task_status)")) {
            st.setString(1, status.name());
            final List<Task> tasks = new ArrayList<>();
            try (var rs = st.executeQuery()) {
                while (rs.next()) {
                    tasks.add(taskFromResultSet(rs));
                }
            }
            return tasks;
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public List<Task> list(String workflowId) throws DaoException {
        try (var conn = storage.connect(); var st = conn.prepareStatement(
                "SELECT " + FIELDS + " FROM task "
                + " WHERE workflow_id = ?")) {
            st.setString(1, workflowId);
            final List<Task> tasks = new ArrayList<>();
            try (var rs = st.executeQuery()) {
                while (rs.next()) {
                    tasks.add(taskFromResultSet(rs));
                }
            }
            return tasks;
        } catch (SQLException | JsonProcessingException e) {
            throw new DaoException(e);
        }
    }

    @Override
    public void update(Task state) throws DaoException {
        try (var conn = storage.connect(); var st = conn.prepareStatement(
                """
                UPDATE task
                SET (status, rc, error_description, servant_id) = (CAST(? AS task_status), ?, ?, ?)
                WHERE workflow_id = ? AND id = ?""")) {
            int paramCount = 0;
            st.setString(++paramCount, state.status().name());
            st.setObject(++paramCount, state.rc());
            st.setString(++paramCount, state.errorDescription());
            st.setString(++paramCount, state.servantId());
            st.setString(++paramCount, state.workflowId());
            st.setString(++paramCount, state.taskId());
            st.execute();
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    @NotNull
    private TaskImpl taskFromResultSet(ResultSet rs) throws SQLException, JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        int resCount = 0;
        final String id = rs.getString(++resCount);
        final String workflowIdRes = rs.getString(++resCount);
        final String workflowName = rs.getString(++resCount);
        final TaskDesc taskDesc = objectMapper.readValue(rs.getString(++resCount), TaskDesc.class);
        final TaskState.Status status = TaskState.Status.valueOf(rs.getString(++resCount));
        final Integer rc = rs.getObject(++resCount, Integer.class);
        final String errorDesc = rs.getString(++resCount);
        final String servantId = rs.getString(++resCount);
        final var state = new TaskState(id, workflowIdRes, workflowName, taskDesc, status, rc, errorDesc, servantId);
        return new TaskImpl(state, this);
    }
}
