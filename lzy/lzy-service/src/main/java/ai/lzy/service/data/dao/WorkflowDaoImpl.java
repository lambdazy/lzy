package ai.lzy.service.data.dao;


import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.storage.LzyServiceStorage;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;

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

    private static final String QUERY_GET_WORKFLOW_INFO = """
        SELECT workflow_name, user_id
        FROM workflows
        WHERE active_execution_id = ?""";

    private final Storage storage;

    public WorkflowDaoImpl(LzyServiceStorage storage) {
        this.storage = storage;
    }

    @Override
    @Nullable
    public String upsert(String ownerId, String workflowName, String executionId,
                         @Nullable TransactionHandle transaction) throws SQLException
    {
        String[] previousExecutionId = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var selectSt = connection.prepareStatement(QUERY_SELECT_WORKFLOW + " FOR UPDATE",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
            {
                selectSt.setString(1, ownerId);
                selectSt.setString(2, workflowName);

                ResultSet rs = selectSt.executeQuery();
                var now = Timestamp.from(Instant.now());

                if (rs.next()) {
                    previousExecutionId[0] = rs.getString("active_execution_id");
                    rs.updateString("active_execution_id", executionId);
                    rs.updateTimestamp("modified_at", now);
                    rs.updateRow();
                } else {
                    try (var insertSt = connection.prepareStatement(QUERY_INSERT_WORKFLOW)) {
                        insertSt.setString(1, workflowName);
                        insertSt.setString(2, ownerId);
                        insertSt.setTimestamp(3, now);
                        insertSt.setTimestamp(4, now);
                        insertSt.setString(5, executionId);
                        insertSt.execute();
                    }
                }
            }
        });

        return previousExecutionId[0];
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
    public void setActiveExecutionToNull(String userId, String workflowName, String executionId,
                                         @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            try (var statement = conn.prepareStatement(QUERY_UPDATE_ACTIVE_EXECUTION + " AND workflow_name = ?")) {
                statement.setString(1, null);
                statement.setTimestamp(2, Timestamp.from(Instant.now()));
                statement.setString(3, userId);
                statement.setString(4, executionId);
                statement.setString(5, workflowName);
                if (statement.executeUpdate() < 1) {
                    LOG.error("Active execution of user not found: { executionId: {}, userId: {} }",
                        executionId, userId);
                    throw new NotFoundException("Cannot find active execution '%s' of user '%s'".formatted(executionId,
                        userId));
                }
            }
        });
    }

    static void setActiveExecutionToNull(String userId, @Nullable String workflowName, String executionId,
                                         LzyServiceStorage storage, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        var queryString = Objects.isNull(workflowName) ? QUERY_UPDATE_ACTIVE_EXECUTION
            : QUERY_UPDATE_ACTIVE_EXECUTION + " AND workflow_name = ?";

        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(queryString)) {
                statement.setString(1, null);
                statement.setTimestamp(2, Timestamp.from(Instant.now()));
                statement.setString(3, userId);
                statement.setString(4, executionId);
                if (workflowName != null) {
                    statement.setString(5, workflowName);
                }
                if (statement.executeUpdate() < 1) {
                    LOG.warn("Active execution of user not found: { executionId: {}, userId: {} }",
                        executionId, userId);
                }
            }
        });
    }
}
