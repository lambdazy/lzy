package ai.lzy.service.data.dao;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.NotFoundException;
import ai.lzy.service.data.ExecutionStatus;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.service.data.PortalStatus;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LMST;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

@Singleton
public class ExecutionDaoImpl implements ExecutionDao {
    private static final Logger LOG = LogManager.getLogger(ExecutionDaoImpl.class);

    private static final Function<String, String> toSqlString = (obj) -> "'" + obj + "'";

    private static final String QUERY_INSERT_EXECUTION = """
        INSERT INTO workflow_executions (execution_id, user_id, created_at, storage,
            storage_uri, storage_credentials, execution_status)
        VALUES (?, ?, ?, ?, ?, ?, cast(? as execution_status))""";

    private static final String QUERY_DELETE_EXECUTION = """
        DELETE FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_PORTAL_CHANNEL_IDS = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), portal_stdout_channel_id = ?, portal_stderr_channel_id = ?
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_ALLOCATOR_SESSION = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), allocator_session_id = ?, portal_id = ?
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_ALLOCATE_OPERATION_DATA = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), allocate_op_id = ?, portal_vm_id = ?
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_ALLOCATE_VM_ADDRESS = """
        UPDATE workflow_executions
        SET portal = cast(? as portal_status), portal_vm_address = ?, portal_fs_address = ?
        WHERE execution_id = ?""";

    private static final String QUERY_GET_EXEC_FINISH_DATA = """
        SELECT execution_id, finished_at, finished_with_error, finished_error_code
        FROM workflow_executions
        WHERE execution_id = ? AND user_id = ?""";

    private static final String QUERY_UPDATE_EXECUTION_FINISH_DATA = """
        UPDATE workflow_executions
        SET finished_at = ?, finished_with_error = ?, finished_error_code = ?
        WHERE execution_id = ?""";

    private static final String QUERY_SELECT_EXECUTION_STATUS = """
        SELECT execution_id, execution_status FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_EXECUTION_STATUS = """
        UPDATE workflow_executions
        SET execution_status = cast(? as execution_status)
        WHERE execution_id = ?""";

    private static final String QUERY_EXISTING_SLOTS_IN_EXECUTION = """
        SELECT slot_uri
        FROM snapshots
        WHERE execution_id = ? AND slot_uri IN %s""";

    private static final String QUERY_PUT_SLOTS_URI = """
        INSERT INTO snapshots (slot_uri, execution_id) 
        VALUES (?, ?)""";

    private static final String QUERY_PUT_CHANNELS = """
        INSERT INTO channels (output_slot_uri, channel_id)
        VALUES (?, ?)""";

    private static final String QUERY_FIND_EXISTING_SLOTS = """
        SELECT slot_uri
        FROM snapshots
        WHERE slot_uri IN %s""";

    private static final String QUERY_FIND_CHANNELS = """
        SELECT output_slot_uri, channel_id
        FROM channels
        WHERE output_slot_uri IN %s""";

    private static final String QUERY_GET_STORAGE_CREDENTIALS = """
        SELECT storage_credentials
        FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_GET_PORTAL_DESCRIPTION = """
        SELECT
          portal,
          allocator_session_id,
          portal_vm_id,
          portal_vm_address,
          portal_fs_address,
          portal_stdout_channel_id,
          portal_stderr_channel_id,
          portal_id
        FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_GET_ALLOCATOR_SESSION = """
        SELECT allocator_session_id
        FROM workflow_executions
        WHERE execution_id = ?
        """;

    private static final String QUERY_PICK_EXPIRED_EXECUTION = """
        SELECT execution_id
        FROM workflow_executions
        WHERE execution_status = 'ERROR'
        LIMIT 1""";

    private static final String QUERY_UPDATE_KAFKA_TOPIC = """
        UPDATE workflow_executions
        SET kafka_topic_json = ?
        WHERE execution_id = ?""";

    private static final String QUERY_GET_KAFKA_TOPIC = """
        SELECT kafka_topic_json
        FROM workflow_executions
        WHERE execution_id = ?""";

    private final LzyServiceStorage storage;
    private final ObjectMapper objectMapper;

    public ExecutionDaoImpl(LzyServiceStorage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void create(String userId, String executionId, String storageName, LMST.StorageConfig storageConfig,
                       @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_INSERT_EXECUTION)) {
                statement.setString(1, executionId);
                statement.setString(2, userId);
                statement.setTimestamp(3, Timestamp.from(Instant.now()));
                statement.setString(4, storageName);
                statement.setString(5, storageConfig.getUri());
                statement.setString(6, objectMapper.writeValueAsString(storageConfig));
                statement.setString(7, ExecutionStatus.RUN.name());
                statement.execute();
            } catch (JsonProcessingException e) {
                LOG.error("Cannot dump storage data of execution: {}", e.getMessage());
                throw new RuntimeException("Cannot dump values", e);
            }
        });
    }

    @Override
    public void delete(String executionId, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_DELETE_EXECUTION)) {
                statement.setString(1, executionId);
                statement.execute();
            }
        });
    }

    @Override
    public void updateStdChannelIds(String executionId, String stdoutChannelId, String stderrChannelId,
                                    @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_UPDATE_PORTAL_CHANNEL_IDS)) {
                statement.setString(1, PortalStatus.CREATING_SESSION.name());
                statement.setString(2, stdoutChannelId);
                statement.setString(3, stderrChannelId);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updatePortalVmAllocateSession(String executionId, String sessionId, String portalId,
                                              @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_UPDATE_ALLOCATOR_SESSION)) {
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
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_UPDATE_ALLOCATE_OPERATION_DATA)) {
                statement.setString(1, PortalStatus.ALLOCATING_VM.name());
                statement.setString(2, opId);
                statement.setString(3, vmId);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updatePortalVmAddress(String executionId, String vmAddress, String fsAddress,
                                      @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_UPDATE_ALLOCATE_VM_ADDRESS)) {
                statement.setString(1, PortalStatus.VM_READY.name());
                statement.setString(2, vmAddress);
                statement.setString(3, fsAddress);
                statement.setString(4, executionId);
                statement.executeUpdate();
            }
        });
    }

    @Override
    public void updateFinishData(String userId, String executionId, Status status, @Nullable TransactionHandle tx)
        throws SQLException
    {
        DbOperation.execute(tx, storage, conn -> {
            try (var getFinishStmt = conn.prepareStatement(QUERY_GET_EXEC_FINISH_DATA + " FOR UPDATE")) {
                getFinishStmt.setString(1, executionId);
                getFinishStmt.setString(2, userId);
                ResultSet rs = getFinishStmt.executeQuery();

                if (rs.next()) {
                    if (rs.getTimestamp("finished_at") != null) {
                        LOG.error("Attempt to finish already finished execution: " +
                                "{ executionId: {}, finished_at: {}, reason: {} }",
                            executionId, rs.getTimestamp("finished_at"), rs.getString("finished_with_error"));
                        throw new IllegalStateException("Workflow execution already finished");
                    }

                    try (var updateStmt = conn.prepareStatement(QUERY_UPDATE_EXECUTION_FINISH_DATA)) {
                        updateStmt.setTimestamp(1, Timestamp.from(Instant.now()));
                        updateStmt.setString(2, status.getDescription());
                        updateStmt.setInt(3, status.getCode().value());
                        updateStmt.setString(4, executionId);

                        updateStmt.executeUpdate();
                    }
                } else {
                    LOG.error("Attempt to finish unknown execution: { userId: {}, executionId: {} }",
                        userId, executionId);
                    throw new NotFoundException("Cannot find execution '%s' of user '%s'".formatted(executionId,
                        userId));
                }
            }
        });
    }

    @Override
    public void setErrorExecutionStatus(String executionId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var selectStmt = connection.prepareStatement(QUERY_SELECT_EXECUTION_STATUS + "FOR UPDATE")) {
                selectStmt.setString(1, executionId);
                ResultSet rs = selectStmt.executeQuery();

                if (rs.next()) {
                    var curStatus = ExecutionStatus.valueOf(rs.getString("execution_status"));
                    if (curStatus != ExecutionStatus.RUN) {
                        LOG.error("Attempt to set status '{}' to '{}' execution: { executionId: {} }",
                            ExecutionStatus.ERROR, curStatus.name(), executionId);
                        throw new IllegalStateException("Invalid execution status change");
                    }

                    try (var updateStmt = connection.prepareStatement(QUERY_UPDATE_EXECUTION_STATUS)) {
                        updateStmt.setString(1, ExecutionStatus.ERROR.name());
                        updateStmt.setString(2, executionId);
                        updateStmt.executeUpdate();
                    }
                } else {
                    LOG.error("Attempt to set status '{}' of unknown execution: { executionId: {} }",
                        ExecutionStatus.ERROR, executionId);
                    throw new NotFoundException("Cannot find execution '%s'".formatted(executionId));
                }
            }
        });
    }

    @Override
    public void setCompletingExecutionStatus(String executionId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_SELECT_EXECUTION_STATUS + "FOR UPDATE")) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var curStatus = ExecutionStatus.valueOf(rs.getString("execution_status"));
                    if (curStatus != ExecutionStatus.ERROR && curStatus != ExecutionStatus.RUN) {
                        LOG.error("Attempt to set status '{}' to '{}' execution: { executionId: {} }",
                            ExecutionStatus.COMPLETING, curStatus.name(), executionId);
                        throw new IllegalStateException("Invalid execution status change");
                    }

                    try (var updateStmt = connection.prepareStatement(QUERY_UPDATE_EXECUTION_STATUS)) {
                        updateStmt.setString(1, ExecutionStatus.COMPLETING.name());
                        updateStmt.setString(2, executionId);
                        updateStmt.executeUpdate();
                    }
                } else {
                    LOG.error("Attempt to set status '{}' of unknown execution: { executionId: {} }",
                        ExecutionStatus.COMPLETING, executionId);
                    throw new NotFoundException("Cannot find execution '%s'".formatted(executionId));
                }
            }
        });
    }

    @Override
    public void setCompletedExecutionStatus(String executionId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_SELECT_EXECUTION_STATUS + "FOR UPDATE",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
            {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var curStatus = ExecutionStatus.valueOf(rs.getString("execution_status"));
                    if (curStatus != ExecutionStatus.COMPLETING) {
                        LOG.error("Attempt to set status '{}' to '{}' execution: { executionId: {} }",
                            ExecutionStatus.COMPLETED, curStatus.name(), executionId);
                        throw new IllegalStateException("Invalid execution status change");
                    }

                    try (var updateStmt = connection.prepareStatement(QUERY_UPDATE_EXECUTION_STATUS)) {
                        updateStmt.setString(1, ExecutionStatus.COMPLETED.name());
                        updateStmt.setString(2, executionId);
                        updateStmt.executeUpdate();
                    }
                } else {
                    LOG.error("Attempt to set status '{}' of unknown execution: { executionId: {} }",
                        ExecutionStatus.COMPLETED, executionId);
                    throw new NotFoundException("Cannot find execution '%s'".formatted(executionId));
                }
            }
        });
    }

    @Override
    public void saveSlots(String executionId, Set<String> slotsUri, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_PUT_SLOTS_URI)) {
                for (var slotUri : slotsUri) {
                    statement.setString(1, slotUri);
                    statement.setString(2, executionId);
                    statement.addBatch();
                    statement.clearParameters();
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public void saveChannels(Map<String, String> slot2channel, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_PUT_CHANNELS)) {
                for (var slotAndChannel : slot2channel.entrySet()) {
                    statement.setString(1, slotAndChannel.getKey());
                    statement.setString(2, slotAndChannel.getValue());
                    statement.addBatch();
                    statement.clearParameters();
                }
                statement.executeBatch();
            }
        });
    }

    @Override
    public Set<String> retainExistingSlots(Set<String> slotsUri) throws SQLException {
        if (slotsUri.isEmpty()) {
            return Collections.emptySet();
        }

        var existingSlots = new HashSet<String>();

        DbOperation.execute(null, storage, con -> {
            var slotsAsString = JsonUtils.printAsTuple(slotsUri, toSqlString);

            try (var statement = con.prepareStatement(QUERY_FIND_EXISTING_SLOTS.formatted(slotsAsString))) {
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    existingSlots.add(rs.getString("slot_uri"));
                }
            }
        });

        return existingSlots;
    }

    @Override
    public Set<String> retainNonExistingSlots(String executionId, Set<String> slotsUri) throws SQLException {
        if (slotsUri.isEmpty()) {
            return Collections.emptySet();
        }

        var existingSlots = new HashSet<String>();

        DbOperation.execute(null, storage, con -> {
            var slotsAsString = JsonUtils.printAsTuple(slotsUri, toSqlString);

            try (var statement = con.prepareStatement(QUERY_EXISTING_SLOTS_IN_EXECUTION.formatted(slotsAsString))) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    existingSlots.add(rs.getString("slot_uri"));
                }
            }
        });

        return SetUtils.difference(slotsUri, existingSlots);
    }

    @Override
    public Map<String, String> findChannels(Set<String> slotsUri) throws SQLException {
        if (slotsUri.isEmpty()) {
            return Collections.emptyMap();
        }

        var existingChannels = new HashMap<String, String>();

        DbOperation.execute(null, storage, con -> {
            var slotsAsString = JsonUtils.printAsTuple(slotsUri, toSqlString);

            try (var statement = con.prepareStatement(QUERY_FIND_CHANNELS.formatted(slotsAsString))) {
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    existingChannels.put(rs.getString("output_slot_uri"), rs.getString("channel_id"));
                }
            }
        });

        return existingChannels;
    }

    @Override
    @Nullable
    public LMST.StorageConfig getStorageConfig(String executionId) throws SQLException {
        LMST.StorageConfig[] credentials = {null};

        DbOperation.execute(null, storage, connection -> {
            try (var statement = connection.prepareStatement(QUERY_GET_STORAGE_CREDENTIALS)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var dump = rs.getString("storage_credentials");
                    if (dump == null) {
                        LOG.warn("Cannot obtain storage credentials for execution: { executionId : {} }", executionId);
                        throw new RuntimeException("Cannot obtain storage credentials");
                    }
                    credentials[0] = objectMapper.readValue(dump, LMST.StorageConfig.class);
                } else {
                    LOG.warn("Storage credentials not found: { executionId : {} }", executionId);
                }
            } catch (JsonProcessingException e) {
                LOG.error("Cannot parse value of storage credentials: {}", e.getMessage());
                throw new RuntimeException("Cannot parse values", e);
            }
        });

        return credentials[0];
    }

    @Override
    @Nullable
    public PortalDescription getPortalDescription(String executionId) throws SQLException {
        PortalDescription[] descriptions = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_PORTAL_DESCRIPTION)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    var status = PortalDescription.PortalStatus.valueOf(rs.getString(1));
                    var allocateSessionId = rs.getString(2);
                    var vmId = rs.getString(3);
                    var vmAddress = rs.getString(4) == null ? null :
                        HostAndPort.fromString(rs.getString(4));
                    var fsAddress = rs.getString(5) == null ? null :
                        HostAndPort.fromString(rs.getString(5));
                    var stdoutChannelId = rs.getString(6);
                    var stderrChannelId = rs.getString(7);
                    var portalId = rs.getString(8);

                    descriptions[0] = new PortalDescription(portalId, allocateSessionId, vmId, vmAddress, fsAddress,
                        stdoutChannelId, stderrChannelId, status);
                } else {
                    LOG.warn("Cannot find portal description: { executionId: {} }", executionId);
                }
            }
        });

        return descriptions[0];
    }

    @Override
    @Nullable
    public String getAllocatorSession(String executionId) throws SQLException {
        String[] res = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_GET_ALLOCATOR_SESSION)) {
                statement.setString(1, executionId);
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    res[0] = rs.getString(1);
                } else {
                    LOG.warn("Cannot find allocator session: { executionId: {} }", executionId);
                }
            }
        });

        return res[0];
    }

    @Override
    @Nullable
    public String getExpiredExecution() throws SQLException {
        String[] res = {null};

        DbOperation.execute(null, storage, con -> {
            try (var statement = con.prepareStatement(QUERY_PICK_EXPIRED_EXECUTION)) {
                ResultSet rs = statement.executeQuery();

                if (rs.next()) {
                    res[0] = rs.getString(1);
                }
            }
        });
        return res[0];
    }

    @Override
    public void setKafkaTopicDesc(String executionId, KafkaTopicDesc topicDesc,
                                  @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement stmt = con.prepareStatement(QUERY_UPDATE_KAFKA_TOPIC)) {
                var mapper = new ObjectMapper().findAndRegisterModules();
                stmt.setString(1, mapper.writeValueAsString(topicDesc));
                stmt.setString(2, executionId);
                stmt.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot dump kafka topic desc", e);
            }
        });
    }

    @Override
    public KafkaTopicDesc getKafkaTopicDesc(String executionId, TransactionHandle transaction) throws SQLException {
        return DbOperation.execute(transaction, storage, con -> {
            try (PreparedStatement stmt = con.prepareStatement(QUERY_GET_KAFKA_TOPIC)) {
                var mapper = new ObjectMapper().findAndRegisterModules();
                stmt.setString(1, executionId);
                var qs = stmt.executeQuery();
                var kafkaJson = qs.getString(1);

                return mapper.readValue(kafkaJson, KafkaTopicDesc.class);

            } catch (JsonProcessingException e) {
                throw new RuntimeException("Cannot deserialize kafka topic desc", e);
            }
        });
    }
}
