package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.WorkflowDao;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Singleton
public class WorkflowDaoImpl implements WorkflowDao {
    private static final Logger LOG = LogManager.getLogger(WorkflowDaoImpl.class);

    private static final String QUERY_WORKFLOW_EXISTS = """
        SELECT 1 from workflows WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_SELECT_WORKFLOW = """
        SELECT workflow_name, user_id, created_at, modified_at, active_execution_id,
               allocator_session_id, allocator_session_deadline
        FROM workflows
        WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_INSERT_WORKFLOW = """
        INSERT INTO workflows (workflow_name, user_id, created_at, modified_at, active_execution_id)
        VALUES (?, ?, ?, ?, ?)""";

    private static final String QUERY_RESET_ACTIVE_EXECUTION = """
        UPDATE workflows
        SET active_execution_id = NULL,
            modified_at = ?
        WHERE user_id = ? AND workflow_name = ?
        RETURNING allocator_session_id""";

    private static final String QUERY_RESET_ACTIVE_EXECUTION_BY_ID = """
        UPDATE workflows
        SET active_execution_id = NULL,
            modified_at = ?
        WHERE workflow_name = ? AND active_execution_id = ?
        RETURNING allocator_session_id""";

    private final LzyServiceStorage storage;

    public WorkflowDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    @Nullable
    public String upsert(String userId, String wfName, String newActiveExecId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Upsert workflow with active execution: { userId: {}, wfName: {}, execId: {} }", userId, wfName,
            newActiveExecId);

        String[] oldExecId = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var selectSt = connection.prepareStatement(QUERY_SELECT_WORKFLOW + " FOR UPDATE",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
            {
                selectSt.setString(1, userId);
                selectSt.setString(2, wfName);

                var rs = selectSt.executeQuery();
                var now = Timestamp.from(Instant.now());

                if (rs.next()) {
                    oldExecId[0] = rs.getString("active_execution_id");
                    rs.updateString("active_execution_id", newActiveExecId);
                    rs.updateTimestamp("modified_at", now);
                    rs.updateRow();
                } else {
                    try (PreparedStatement insertSt = connection.prepareStatement(QUERY_INSERT_WORKFLOW)) {
                        insertSt.setString(1, wfName);
                        insertSt.setString(2, userId);
                        insertSt.setTimestamp(3, now);
                        insertSt.setTimestamp(4, now);
                        insertSt.setString(5, newActiveExecId);
                        insertSt.execute();
                    }
                }
            }
        });

        return oldExecId[0];
    }

    @Override
    public boolean exists(String userId, String wfName) throws SQLException {
        return DbOperation.execute(null, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_WORKFLOW_EXISTS)) {
                st.setString(1, userId);
                st.setString(2, wfName);
                return st.executeQuery().next();
            }
        });
    }

    @Override
    @Nullable
    public String getExecutionId(String userId, String wfName, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_SELECT_WORKFLOW + forUpdate(transaction))) {
                st.setString(1, userId);
                st.setString(2, wfName);
                var rs = st.executeQuery();
                if (rs.next()) {
                    return rs.getString("active_execution_id");
                } else {
                    throw new NotFoundException("Cannot found workflow with name='%s'".formatted(wfName));
                }
            }
        });
    }

    @Override
    public boolean cleanActiveExecutionById(String wfName, String activeExecId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Try to deactivate workflow with broken execution: { brokenExecId: {} }", activeExecId);
        return DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_RESET_ACTIVE_EXECUTION_BY_ID)) {
                st.setTimestamp(1, Timestamp.from(Instant.now()));
                st.setString(2, wfName);
                st.setString(3, activeExecId);
                var rs = st.executeQuery();
                if (rs.next()) {

                }
                return null;
            }
        });
    }

    @Override
    public void cleanActiveExecution(String userId, String wfName, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Reset active execution of workflow: { userId: {}, wfName: {} }", userId, wfName);
        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_RESET_ACTIVE_EXECUTION)) {
                st.setTimestamp(1, Timestamp.from(Instant.now()));
                st.setString(2, userId);
                st.setString(3, wfName);

                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot update execution of unknown workflow: { userId: {}, wfName: {} }", userId,
                        wfName);
                    throw new RuntimeException("Workflow with name='%s' not found".formatted(wfName));
                }
            }
        });
    }

    @Nullable
    @Override
    public String acquireCurrentAllocatorSession(String userId, String wfName) throws SQLException {
        LOG.debug("Try to acquire current allocator session for { userId: {}, wfName: {} }", userId, wfName);

        return DbOperation.execute(null, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement("""
                UPDATE workflows
                SET allocator_session_deadline = NULL
                WHERE user_id = ? AND workflow_name = ? AND allocator_session_id IS NOT NULL
                RETURNING allocator_session_id
                """))
            {
                st.setString(1, userId);
                st.setString(2, wfName);

                var rs = st.executeQuery();
                if (rs.next()) {
                    return rs.getString(1);
                }

                return null;
            }
        });
    }

    @Nullable
    @Override
    public String setAllocatorSessionId(String userId, String wfName, String sessionId) throws SQLException {
        LOG.debug("Try to set allocator session for { userId: {}, wfName: {} } to {}", userId, wfName, sessionId);

        try (var tx = TransactionHandle.create(storage);
             var conn = tx.connect())
        {
            try (PreparedStatement st = conn.prepareStatement("""
                UPDATE workflows
                SET allocator_session_id = ?, allocator_session_deadline = NULL
                WHERE user_id = ? AND workflow_name = ? AND allocator_session_id IS NULL
                """))
            {
                st.setString(1, sessionId);
                st.setString(2, userId);
                st.setString(3, wfName);

                var updated = st.executeUpdate();
                if (updated == 1) {
                    tx.commit();
                    return null; // all is ok, new `sid` is set
                }

                assert updated == 0;
            }

            try (PreparedStatement st = conn.prepareStatement("""
                SELECT allocator_session_id, allocator_session_deadline
                FROM workflows
                WHERE user_id = ? AND workflow_name = ?
                """))
            {
                st.setString(1, userId);
                st.setString(2, wfName);

                var rs = st.executeQuery();
                tx.commit();

                if (rs.next()) {
                    var actualAllocatorSessionId = rs.getString(1);
                    var actualAllocatorSessionDeadline = rs.getTimestamp(2);

                    assert actualAllocatorSessionDeadline == null;
                    return actualAllocatorSessionId;
                }

                LOG.error("Smth went wrong, userId: {}, wf: {}", userId, wfName);
                throw new RuntimeException("Smth went wrong");
            }
        }
    }

    @Override
    public boolean cleanAllocatorSessionId(String userId, String wfName, String sessionId,
                                           @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement("""
                UPDATE workflows
                SET allocator_session_id = NULL, allocator_session_deadline = NULL
                WHERE user_id = ? AND workflow_name = ? AND allocator_session_id = ?
                  AND allocator_session_deadline IS NOT NULL AND allocator_session_deadline < ?"""))
            {
                st.setString(1, userId);
                st.setString(2, wfName);
                st.setString(3, sessionId);
                st.setTimestamp(4, Timestamp.from(Instant.now()));
                return st.executeUpdate() > 0;
            }
        });
    }

    @Override
    public List<OutdatedAllocatorSession> listOutdatedAllocatorSessions(int limit) throws SQLException {
        try (var conn = storage.connect();
             PreparedStatement st = conn.prepareStatement("""
                SELECT user_id, workflow_name, allocator_session_id
                FROM workflows
                WHERE allocator_session_id IS NOT NULL
                  AND allocator_session_deadline IS NOT NULL
                  AND allocator_session_deadline < ?
                LIMIT
                """ + limit))
        {
            st.setTimestamp(1, Timestamp.from(Instant.now()));
            var rs = st.executeQuery();

            var result = new ArrayList<OutdatedAllocatorSession>(limit);
            while (rs.next()) {
                result.add(new OutdatedAllocatorSession(
                    rs.getString(1),
                    rs.getString(2),
                    rs.getString(3)
                ));
            }

            return result;
        }
    }

    @Nullable
    @Override
    public WorkflowDesc loadWorkflowDescForTests(String userId, String wfName) throws SQLException {
        return DbOperation.execute(null, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement("""
                SELECT allocator_session_id, allocator_session_deadline
                FROM workflows
                WHERE user_id = ? AND workflow_name = ?"""))
            {
                st.setString(1, userId);
                st.setString(2, wfName);
                var rs = st.executeQuery();
                if (rs.next()) {
                    return new WorkflowDesc(
                        userId,
                        wfName,
                        rs.getString(1),
                        Optional.ofNullable(rs.getTimestamp(2)).map(Timestamp::toInstant).orElse(null));
                }
                return null;
            }
        });
    }

    private static String forUpdate(@Nullable TransactionHandle tx) {
        return tx != null ? " FOR UPDATE" : "";
    }
}
