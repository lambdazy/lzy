package ai.lzy.service.data.dao;


import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.ExecutionStatus;
import ai.lzy.service.data.PortalStatus;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.v1.common.LMS3;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Locale;
import javax.annotation.Nullable;

@Singleton
public class WorkflowDaoImpl implements WorkflowDao {
    private static final Logger LOG = LogManager.getLogger(WorkflowDaoImpl.class);

    private static final String QUERY_GET_ACTIVE_EXECUTION_ID = """
        SELECT active_execution_id
        FROM workflows
        WHERE user_id = ? AND workflow_name = ?""";

    private static final String QUERY_INSERT_WORKFLOW = """
        INSERT INTO workflows (user_id, workflow_name, created_at, active_execution_id)
        VALUES (?, ?, ?, ?)""";

    private static final String QUERY_GET_WORKFLOW_NAME = """
        SELECT workflow_name
        FROM workflows
        WHERE active_execution_id = ?""";

    private static final String QUERY_GET_USER_ID = """
        SELECT user_id
        FROM workflows
        WHERE active_execution_id = ?""";

    private static final String QUERY_EXISTS_ACTIVE_EXECUTION_FOR_WORKFLOW = """
        SELECT 1 FROM workflows
        WHERE user_id = ? AND workflow_name = ? AND active_execution_id = ?""";

    private static final String QUERY_UPDATE_ACTIVE_EXECUTION = """
        UPDATE workflows
        SET active_execution_id = ?
        WHERE user_id = ? AND workflow_name=?""";

    private static final String QUERY_INSERT_EXECUTION = """
        INSERT INTO workflow_executions (execution_id, created_at, storage, storage_bucket, 
            storage_credentials, execution_status)
        VALUES (?, ?, cast(? as storage_type), ?, ?, cast(? as execution_status))""";

    private static final String QUERY_UPDATE_PORTAL_STATUS = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status)
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_PORTAL_CHANNEL_IDS = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), portal_stdout_channel_id = ?, portal_stderr_channel_id = ?
        WHERE execution_id = ?""";

    public static final String QUERY_UPDATE_ALLOCATOR_SESSION = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), allocator_session_id = ?, portal_id = ?
        WHERE execution_id = ?""";

    public static final String QUERY_UPDATE_ALLOCATE_OPERATION_DATA = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), allocate_op_id = ?, portal_vm_id = ?
        WHERE execution_id = ?""";

    public static final String QUERY_UPDATE_ALLOCATE_VM_ADDRESS = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), portal_vm_address = ?, portal_fs_address = ?
        WHERE execution_id = ?""";

    public static final String QUERY_SELECT_EXECUTION_ERROR = """
        SELECT finished_with_error, finished_error_code
        FROM workflow_executions
        WHERE execution_id = ?""";
    public static final String QUERY_GET_EXECUTION_FINISH_DATA = """
        SELECT execution_id, finished_at, finished_with_error, finished_error_code
        FROM workflow_executions
        WHERE execution_id = ?""";

    public static final String QUERY_UPDATE_EXECUTION_FINISH_DATA = """
        UPDATE workflow_executions
        SET finished_at = ?, finished_with_error = ?, finished_error_code = ?,
        execution_status = cast(? as execution_status)
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_CLEANED_EXECUTION = """
        UPDATE workflow_executions
        SET execution_status = cast(? as execution_status)
        WHERE execution_id = ?""";

    public static final String QUERY_GET_PORTAL_ADDRESS = """
        SELECT portal_vm_address
        FROM workflow_executions
        WHERE execution_id = ?""";

    public static final String QUERY_GET_STORAGE_CREDENTIALS = """
        SELECT storage_credentials
        FROM workflow_executions
        WHERE execution_id = ?""";

    public static final String QUERY_GET_ALLOCATOR_SESSION = """
        SELECT allocator_session_id
        FROM workflow_executions
        WHERE execution_id = ?
        """;

    public static final String QUERY_GET_PORTAL_DESCRIPTION = """
        SELECT
          portal,
          portal_vm_id,
          portal_vm_address,
          portal_fs_address,
          portal_stdout_channel_id,
          portal_stderr_channel_id,
          portal_id
        FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_LIST_EXPIRED_EXECUTIONS = """
        SELECT execution_id
        FROM workflow_executions
        WHERE execution_status = 'ERROR'
        LIMIT 1""";

    private final Storage storage;
    private final ObjectMapper objectMapper;

    public WorkflowDaoImpl(LzyServiceStorage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void create(String executionId, String userId, String workflowName, String storageType,
                       LMS3.S3Locator storageData, @Nullable TransactionHandle outerTransaction)
        throws SQLException
    {
        try (var transaction = TransactionHandle.getOrCreate(storage, outerTransaction)) {
            DbOperation.execute(transaction, storage, con -> {
                // TODO: add `nowait` and handle it's warning or error
                var activeExecStmt = con.prepareStatement(QUERY_GET_ACTIVE_EXECUTION_ID + " FOR UPDATE");
                activeExecStmt.setString(1, userId);
                activeExecStmt.setString(2, workflowName);

                boolean update = false;
                String existingExecutionId = null;
                try (var rs = activeExecStmt.executeQuery()) {
                    if (rs.next()) {
                        existingExecutionId = rs.getString("active_execution_id");
                        if (StringUtils.isNotEmpty(existingExecutionId)) {
                            throw new AlreadyExistsException(
                                String.format("Attempt to start one more instance of workflow: active is '%s'",
                                existingExecutionId)
                            );
                        }
                        update = true;
                    }
                }

                try (var statement = con.prepareStatement(QUERY_INSERT_EXECUTION)) {
                    statement.setString(1, executionId);
                    statement.setTimestamp(2, Timestamp.from(Instant.now()));
                    statement.setString(3, storageType.toUpperCase(Locale.ROOT));
                    statement.setString(4, storageData.getBucket());
                    statement.setString(5, objectMapper.writeValueAsString(storageData));
                    statement.setString(6, ExecutionStatus.RUN.name());
                    statement.executeUpdate();
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Cannot dump values", e);
                }

                if (update) {
                    try (var statement = con.prepareStatement(QUERY_UPDATE_ACTIVE_EXECUTION)) {
                        statement.setString(1, executionId);
                        statement.setString(2, userId);
                        statement.setString(3, workflowName);

                        var res = statement.executeUpdate();
                        if (res != 1) {
                            throw new RuntimeException("Active execution was not updated");
                        }
                    }
                    return;
                }

                try (var statement = con.prepareStatement(QUERY_INSERT_WORKFLOW)) {
                    statement.setString(1, userId);
                    statement.setString(2, workflowName);
                    statement.setTimestamp(3, Timestamp.from(Instant.now()));
                    statement.setString(4, executionId);
                    statement.executeUpdate();
                }
            });

            transaction.commit();
        }
    }

    @Override
    public boolean doesActiveExecutionExists(String userId, String workflowName, String executionId)
        throws SQLException
    {
        boolean[] result = {false};
        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_EXISTS_ACTIVE_EXECUTION_FOR_WORKFLOW)) {
                statement.setString(1, userId);
                statement.setString(2, workflowName);
                statement.setString(3, executionId);
                result[0] = statement.executeQuery().next();
            }
        });
        return result[0];
    }

    @Override
    public void updateStatus(String executionId, PortalStatus portalStatus,
                             @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_PORTAL_STATUS)) {
                statement.setString(1, PortalStatus.CREATING_STD_CHANNELS.name());
                statement.setString(2, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateStdChannelIds(String executionId, String stdoutChannelId, String stderrChannelId,
                                    @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_PORTAL_CHANNEL_IDS)) {
                statement.setString(1, PortalStatus.CREATING_SESSION.name());
                statement.setString(2, stdoutChannelId);
                statement.setString(3, stderrChannelId);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateAllocatorSession(String executionId, String sessionId, String portalId,
                                       @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_ALLOCATOR_SESSION)) {
                statement.setString(1, PortalStatus.REQUEST_VM.name());
                statement.setString(2, sessionId);
                statement.setString(3, portalId);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateAllocateOperationData(String executionId, String opId, String vmId,
                                            @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_ALLOCATE_OPERATION_DATA)) {
                statement.setString(1, PortalStatus.ALLOCATING_VM.name());
                statement.setString(2, opId);
                statement.setString(3, vmId);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateAllocatedVmAddress(String executionId, String vmAddress, String fsAddress,
                                         @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_ALLOCATE_VM_ADDRESS)) {
                statement.setString(1, PortalStatus.VM_READY.name());
                statement.setString(2, vmAddress);
                statement.setString(3, fsAddress);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Nullable
    @Override
    public Status getExecutionErrorStatus(String executionId)
        throws SQLException
    {
        Status[] res = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_SELECT_EXECUTION_ERROR)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var message = rs.getString(1);
                    Integer code = rs.getObject(2, Integer.class);

                    res[0] = message != null && code != null ?
                        Status.fromCodeValue(code).withDescription(message) : null;
                }
            }
        });
        return res[0];
    }

    @Override
    public void updateFinishData(String workflowName, String executionId, Timestamp finishedAt,
                                 @Nullable String finishedWithError, @Nullable Integer finishedErrorCode,
                                 @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            var finishDataStmt = con.prepareStatement(QUERY_GET_EXECUTION_FINISH_DATA + " FOR UPDATE",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            finishDataStmt.setString(1, executionId);

            ResultSet rs = finishDataStmt.executeQuery();
            if (rs.next()) {
                if (rs.getTimestamp("finished_at") != null) {
                    LOG.warn("Attempt to finish already finished workflow '{}' ('{}'). " +
                            "Finished at '{}' with reason '{}'", executionId, workflowName,
                        rs.getTimestamp("finished_at"), rs.getString("finished_with_error"));
                    throw new RuntimeException("Already finished");
                }

                var status = finishedWithError == null ? ExecutionStatus.COMPLETED : ExecutionStatus.ERROR;
                try (var statement = con.prepareStatement(QUERY_UPDATE_EXECUTION_FINISH_DATA)) {
                    statement.setTimestamp(1, finishedAt);
                    statement.setString(2, finishedWithError);
                    statement.setObject(3, finishedErrorCode, java.sql.Types.INTEGER);
                    statement.setString(4, status.name());
                    statement.setString(5, executionId);

                    statement.executeUpdate();
                }
            } else {
                LOG.warn("Attempt to finish unknown workflow '{}' ('{}')", executionId, workflowName);
                throw new RuntimeException("Unknown workflow execution");
            }
        });
    }

    @Override
    public void setDeadExecutionStatus(String executionId,
                                       @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_CLEANED_EXECUTION)) {
                statement.setString(1, ExecutionStatus.CLEANED.name());
                statement.setString(2, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateActiveExecution(String userId, String workflowName, String oldExecutionId,
                                      @Nullable String newExecutionId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_UPDATE_ACTIVE_EXECUTION)) {
                statement.setString(1, newExecutionId);
                statement.setString(2, userId);
                statement.setString(3, workflowName);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public String getUserId(String executionId) throws SQLException {
        String[] userId = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_USER_ID)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    userId[0] = rs.getString("user_id");

                }
                if (userId[0] == null) {
                    LOG.error("Cannot find userId for execution: { executionId: {} }", executionId);
                    throw new NotFoundException("Cannot find userId");
                }
            }
        });

        return userId[0];
    }

    @Override
    public String getWorkflowName(String executionId) throws SQLException {
        String[] workflowName = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_WORKFLOW_NAME)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    workflowName[0] = rs.getString("workflow_name");

                }
                if (Strings.isBlank(workflowName[0])) {
                    LOG.error("Cannot find workflow name for execution: { executionId: {} }", executionId);
                    throw new NotFoundException("Cannot find workflow name");
                }
            }
        });

        return workflowName[0];
    }

    @Override
    public String getPortalAddress(String executionId) throws SQLException {
        String[] portalAddress = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_PORTAL_ADDRESS)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    portalAddress[0] = rs.getString("portal_vm_address");
                }
                if (portalAddress[0] == null) {
                    LOG.warn("Cannot obtain portal vm address for execution: { executionId : {} }", executionId);
                    throw new NotFoundException("Cannot obtain portal address");
                }
            }
        });

        return portalAddress[0];
    }

    @Override
    public LMS3.S3Locator getStorageLocator(String executionId) throws SQLException {
        LMS3.S3Locator[] credentials = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_STORAGE_CREDENTIALS)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var dump = rs.getString("storage_credentials");
                    if (dump == null) {
                        LOG.warn("Cannot obtain storage credentials for execution: { executionId : {} }", executionId);
                        throw new RuntimeException("Cannot obtain storage credentials");
                    }
                    credentials[0] = objectMapper.readValue(dump, LMS3.S3Locator.class);
                } else {
                    LOG.warn("Cannot obtain storage credentials for execution: { executionId : {} }", executionId);
                    throw new RuntimeException("Cannot obtain storage credentials");
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot parse values", e);
            }
        });

        return credentials[0];
    }

    @Nullable
    @Override
    public PortalDescription getPortalDescription(String executionId) throws SQLException {
        PortalDescription[] descriptions = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_PORTAL_DESCRIPTION)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var status = PortalDescription.PortalStatus.valueOf(rs.getString(1));
                    var vmId = rs.getString(2);
                    var vmAddress = rs.getString(3) == null ? null :
                        HostAndPort.fromString(rs.getString(3));
                    var fsAddress = rs.getString(4) == null ? null :
                        HostAndPort.fromString(rs.getString(4));
                    var stdoutChannelId = rs.getString(5);
                    var stderrChannelId = rs.getString(6);
                    var portalId = rs.getString(7);

                    descriptions[0] = new PortalDescription(portalId, vmId, vmAddress, fsAddress,
                            stdoutChannelId, stderrChannelId, status);
                }
            }
        });
        return descriptions[0];
    }

    @Nullable
    public String getAllocatorSession(String executionId) throws SQLException {
        String[] res = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_ALLOCATOR_SESSION)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    res[0] = rs.getString(1);
                }
            }
        });
        return res[0];
    }

    @Override
    public String getExpiredExecution() throws SQLException {
        String[] res = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_LIST_EXPIRED_EXECUTIONS)) {
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    res[0] = rs.getString(1);
                }
            }
        });
        return res[0];
    }
}
