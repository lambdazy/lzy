package ai.lzy.scheduler.db;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class TaskDaoImpl implements TaskDao {

    private static final String FIELDS = "id, execution_id, workflow_name, user_id, operation_id, operation_name";

    private final SchedulerDataSource storage;

    public TaskDaoImpl(SchedulerDataSource storage) {
        this.storage = storage;
    }

    @Nullable
    @Override
    public String getAllocatorSession(String workflowName, String userId,
                                      @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement("""
                SELECT allocator_session_id FROM workflow
                WHERE name = ? AND user_id = ?
                """))
            {
                ps.setString(1, workflowName);
                ps.setString(2, userId);

                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }
                return rs.getString(1);
            }
        });
    }

    @Override
    public void insertAllocatorSession(String workflowName, String userId, String sessionId,
                                       @Nullable TransactionHandle tx) throws SQLException
    {
        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement("""
                INSERT INTO workflow (name, user_id, allocator_session_id)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """))
            {
                ps.setString(1, workflowName);
                ps.setString(2, userId);
                ps.setString(3, sessionId);

                ps.execute();
            }
        });
    }

    @Nullable
    @Override
    public TaskDesc getTaskDesc(String taskId, String executionId,
                                @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement(String.format("""
                SELECT %s FROM task
                WHERE id = ? AND execution_id = ?
                """, FIELDS)))
            {
                ps.setString(1, taskId);
                ps.setString(2, executionId);

                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                var workflowName = rs.getString(3);
                var userId = rs.getString(4);
                var operationId = rs.getString(5);
                var opName = rs.getString(6);

                return new TaskDesc(taskId, executionId, workflowName, userId, operationId, opName);
            }
        });
    }

    @Nullable
    @Override
    public TaskDesc getTaskDesc(String operationId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement(String.format("""
                SELECT %s FROM task
                WHERE operation_id = ?
                """, FIELDS)))
            {
                ps.setString(1, operationId);

                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                var id = rs.getString(1);
                var execId = rs.getString(2);
                var workflowName = rs.getString(3);
                var userId = rs.getString(4);
                var opName = rs.getString(6);

                return new TaskDesc(id, execId, workflowName, userId, operationId, opName);
            }
        });
    }

    @Override
    public void insertTaskDesc(TaskDesc desc, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement(String.format("""
                INSERT INTO task (%s)
                VALUES (?, ?, ?, ?, ?, ?)
                """, FIELDS)))
            {
                ps.setString(1, desc.taskId());
                ps.setString(2, desc.executionId());
                ps.setString(3, desc.workflowName());
                ps.setString(4, desc.userId());
                ps.setString(5, desc.operationId());
                ps.setString(6, desc.operationName());

                ps.execute();
            }
        });
    }

    @Override
    public List<TaskDesc> listTasks(String executionId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement(String.format("""
                SELECT %s FROM task
                WHERE execution_id = ?
                """, FIELDS)))
            {
                ps.setString(1, executionId);

                var rs = ps.executeQuery();

                final ArrayList<TaskDesc> descList = new ArrayList<>();

                while (!rs.next()) {
                    var id = rs.getString(1);
                    var workflowName = rs.getString(3);
                    var userId = rs.getString(4);
                    var operationId = rs.getString(5);
                    var opName = rs.getString(6);

                    descList.add(new TaskDesc(id, executionId, workflowName, userId, operationId, opName));
                }

                return descList;
            }
        });
    }

    @Override
    public List<TaskDesc> listByWfName(String wfName, String userId,
                                       @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, con -> {
            try (PreparedStatement ps = con.prepareStatement(String.format("""
                SELECT %s FROM task
                WHERE workflow_name = ? AND user_id = ?
                """, FIELDS)))
            {
                ps.setString(1, wfName);
                ps.setString(2, userId);

                var rs = ps.executeQuery();

                final ArrayList<TaskDesc> descList = new ArrayList<>();

                while (!rs.next()) {
                    var id = rs.getString(1);
                    var execId = rs.getString(2);
                    var operationId = rs.getString(5);
                    var operationName = rs.getString(6);

                    descList.add(new TaskDesc(id, execId, wfName, userId, operationId, operationName));
                }

                return descList;
            }
        });
    }
}
