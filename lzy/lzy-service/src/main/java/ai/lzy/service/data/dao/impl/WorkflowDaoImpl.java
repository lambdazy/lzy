package ai.lzy.service.data.dao.impl;


import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.dao.WorkflowDao;
import ai.lzy.service.data.storage.LzyServiceStorage;
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

    private static final String QUERY_SELECT_WORKFLOW = """
        SELECT workflow_name, user_id, created_at, modified_at, active_execution_id
        FROM workflows
        WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_INSERT_WORKFLOW = """
        INSERT INTO workflows (workflow_name, user_id, created_at, modified_at, active_execution_id)
        VALUES (?, ?, ?, ?, ?)""";

    private static final String QUERY_UPDATE_ACTIVE_EXECUTION = """
        UPDATE workflows
        SET active_execution_id = ?, modified_at = ? 
        WHERE user_id = ? AND active_execution_id = ?""";

    private static final String QUERY_SET_ACTIVE_EXEC_NULL = """
        UPDATE workflows SET active_execution_id = ?, modified_at = ?
        WHERE user_id = ? AND workflow_name = ? AND active_execution_id = ?
        """;

    private static final String SELECT_FOR_UPDATE_ACTIVE_EXECUTION_BY_WF_NAME = """
        SELECT user_id, workflow_name, modified_at, active_execution_id 
        FROM workflows
        WHERE user_id = ? AND workflow_name = ? 
        FOR UPDATE""";

    private static final String QUERY_GET_WORKFLOW_INFO = """
        SELECT workflow_name, user_id
        FROM workflows
        WHERE active_execution_id = ?""";

    private final LzyServiceStorage storage;

    public WorkflowDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    @Nullable
    public String upsert(String userId, String workflowName, String newActiveExecId,
                         @Nullable TransactionHandle transaction) throws SQLException
    {
        String[] previousExecutionId = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var selectSt = connection.prepareStatement(QUERY_SELECT_WORKFLOW + " FOR UPDATE",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
            {
                selectSt.setString(1, userId);
                selectSt.setString(2, workflowName);

                ResultSet rs = selectSt.executeQuery();
                var now = Timestamp.from(Instant.now());

                if (rs.next()) {
                    previousExecutionId[0] = rs.getString("active_execution_id");
                    rs.updateString("active_execution_id", newActiveExecId);
                    rs.updateTimestamp("modified_at", now);
                    rs.updateRow();
                } else {
                    try (var insertSt = connection.prepareStatement(QUERY_INSERT_WORKFLOW)) {
                        insertSt.setString(1, workflowName);
                        insertSt.setString(2, userId);
                        insertSt.setTimestamp(3, now);
                        insertSt.setTimestamp(4, now);
                        insertSt.setString(5, newActiveExecId);
                        insertSt.execute();
                    }
                }
            }
        });

        return previousExecutionId[0];
    }

    @Override
    @Nullable
    public String getExecutionId(String userId, String wfName, TransactionHandle transaction) throws SQLException {
        String[] activeExecutionId = {null};
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_SELECT_WORKFLOW)) {
                statement.setString(1, userId);
                statement.setString(2, wfName);
                var rs = statement.executeQuery();
                if (rs.next()) {
                    activeExecutionId[0] = rs.getString("active_execution_id");
                } else {
                    throw new NotFoundException("Cannot found workflow of user");
                }
            }
        });
        return activeExecutionId[0];
    }

    @Override
    @Nullable
    public WorkflowInfo findWorkflowBy(String executionId) throws SQLException {
        WorkflowInfo[] result = {null};


        DbOperation.execute(null, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_GET_WORKFLOW_INFO)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    result[0] = new WorkflowInfo(rs.getString("workflow_name"), rs.getString("user_id"));
                } else {
                    LOG.warn("Cannot find workflow with active execution: { executionId: {} }", executionId);
                }
            }
        });

        return result[0];
    }

    @Override
    public boolean deactivate(String userId, String workflowName, String executionId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        boolean[] success = {false};
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_SET_ACTIVE_EXEC_NULL)) {
                statement.setString(1, userId);
                statement.setString(2, workflowName);
                statement.setString(3, executionId);
                success[0] = statement.executeUpdate() > 0;
            }
        });
        return success[0];
    }

    @Override
    public void deactivateA(String userId, String workflowName, String executionId,
                            @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var stmt = connection.prepareStatement(SELECT_FOR_UPDATE_ACTIVE_EXECUTION_BY_WF_NAME,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
            {
                stmt.setString(1, userId);
                stmt.setString(2, workflowName);
                ResultSet rs = stmt.executeQuery();

                if (rs.next()) {
                    var activeExecutionId = rs.getString("active_execution_id");

                    if (executionId.equals(activeExecutionId)) {
                        rs.updateString("active_execution_id", null);
                        rs.updateTimestamp("modified_at", Timestamp.from(Instant.now()));
                        rs.updateRow();
                    }
                } else {
                    throw new NotFoundException("User workflow not found");
                }
            }
        });
    }

    @Override
    public void deactivateA(String userId, String executionId, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, con -> {
            try (var stmt = con.prepareStatement(QUERY_UPDATE_ACTIVE_EXECUTION)) {
                stmt.setString(1, executionId);
                stmt.setTimestamp(2, Timestamp.from(Instant.now()));
                stmt.setString(3, userId);
                stmt.setString(4, executionId);

                if (stmt.executeUpdate() < 1) {
                    LOG.error("Active execution of user not found: { executionId: {}, userId: {} }",
                        executionId, userId);
                    throw new NotFoundException("User workflow not found");
                }
            }
        });
    }
}
