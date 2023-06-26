package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.dao.WorkflowDao;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

@Singleton
public class WorkflowDaoImpl implements WorkflowDao {
    private static final Logger LOG = LogManager.getLogger(WorkflowDaoImpl.class);

    private static final String QUERY_WORKFLOW_EXISTS = """
        SELECT 1 from workflows WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_SELECT_WORKFLOW = """
        SELECT workflow_name, user_id, created_at, modified_at, active_execution_id
        FROM workflows
        WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_INSERT_WORKFLOW = """
        INSERT INTO workflows (workflow_name, user_id, created_at, modified_at, active_execution_id)
        VALUES (?, ?, ?, ?, ?)""";

    private static final String QUERY_UPDATE_ACTIVE_EXECUTION = """
        UPDATE workflows SET active_execution_id = ?, modified_at = ?
        WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_UPDATE_ACTIVE_EXECUTION_TO_NULL = """
        UPDATE workflows SET active_execution_id = NULL, modified_at = ?
        WHERE workflow_name = ? AND active_execution_id = ?""";

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
                    try (var insertSt = connection.prepareStatement(QUERY_INSERT_WORKFLOW)) {
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
            try (var st = connection.prepareStatement(QUERY_SELECT_WORKFLOW)) {
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
    public boolean setActiveExecutionIdToNull(String wfName, String activeExecId,
                                              @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Try to deactivate workflow with broken execution: { brokenExecId: {} }", activeExecId);
        return DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_UPDATE_ACTIVE_EXECUTION_TO_NULL)) {
                st.setTimestamp(1, Timestamp.from(Instant.now()));
                st.setString(2, wfName);
                st.setString(3, activeExecId);
                return st.executeUpdate() > 0;
            }
        });
    }

    @Override
    public void setActiveExecutionId(String userId, String wfName, @Nullable String execId,
                                     @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Update active execution of workflow: { userId: {}, wfName: {}, activeExecId: {} }", userId,
            wfName, execId);
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_UPDATE_ACTIVE_EXECUTION)) {
                st.setString(1, execId);
                st.setTimestamp(2, Timestamp.from(Instant.now()));
                st.setString(3, userId);
                st.setString(4, wfName);
                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot update execution of unknown workflow: { userId: {}, wfName: {} }", userId,
                        wfName);
                    throw new RuntimeException("Workflow with name='%s' not found".formatted(wfName));
                }
            }
        });
    }
}
