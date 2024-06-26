package ai.lzy.service.dao.impl;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.service.dao.ExecutionDao;
import ai.lzy.service.dao.StopExecutionState;
import ai.lzy.v1.common.LMST;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

@Singleton
public class ExecutionDaoImpl implements ExecutionDao {
    private static final Logger LOG = LogManager.getLogger(ExecutionDaoImpl.class);

    private static final String QUERY_INSERT_EXECUTION = """
        INSERT INTO workflow_executions (execution_id, user_id, created_at, storage, storage_uri, storage_credentials)
        VALUES (?, ?, ?, ?, ?, ?)""";

    private static final String QUERY_SELECT_EXECUTION = """
        SELECT 1 FROM workflow_executions WHERE user_id = ? AND execution_id = ?""";

    private static final String QUERY_UPDATE_KAFKA_TOPIC = """
        UPDATE workflow_executions
        SET kafka_topic_json = ?
        WHERE execution_id = ?""";

    private static final String QUERY_UPDATE_ALLOCATOR_SESSION = """
        UPDATE workflow_executions
        SET allocator_session_id = ?
        WHERE execution_id = ?""";

    private static final String QUERY_SELECT_FOR_UPDATE_FINISH_DATA = """
        SELECT execution_id, finished_at, finished_with_error, finished_error_code
        FROM workflow_executions
        WHERE execution_id = ? FOR UPDATE""";

    private static final String QUERY_SELECT_STORAGE_CREDENTIALS = """
        SELECT storage_credentials
        FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_SELECT_STOP_EXECUTION_DATA = """
        SELECT kafka_topic_json, allocator_session_id
        FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_SELECT_ONE_EXPIRED_EXECUTION = """
        SELECT execution_id
        FROM workflow_executions
        WHERE (created_at BETWEEN NOW() - '7 days'::interval AND NOW()) AND (finished_at IS NULL)
        LIMIT 1""";

    private static final String QUERY_SELECT_KAFKA_TOPIC = """
        SELECT kafka_topic_json
        FROM workflow_executions
        WHERE execution_id = ?""";

    private static final String QUERY_SELECT_EXEC_GRAPH_DATA = """
        SELECT kafka_topic_json, storage_credentials, allocator_session_id
        FROM workflow_executions
        WHERE execution_id = ?""";

    private final LzyServiceStorage storage;
    private final ObjectMapper objectMapper;

    public ExecutionDaoImpl(LzyServiceStorage storage, ObjectMapper objectMapper) {
        this.storage = storage;
        this.objectMapper = objectMapper;
    }

    @Override
    public void create(String userId, String execId, String storageName, LMST.StorageConfig storageConfig,
                       @Nullable TransactionHandle transaction) throws SQLException
    {
        LOG.debug("Create execution: { userId: {}, execId: {}, storageName: {}, storageCfg: {} }", userId, execId,
            storageName, safePrinter().printToString(storageConfig));
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_INSERT_EXECUTION)) {
                st.setString(1, execId);
                st.setString(2, userId);
                st.setTimestamp(3, Timestamp.from(Instant.now()));
                st.setString(4, storageName);
                st.setString(5, storageConfig.getUri());
                st.setString(6, objectMapper.writeValueAsString(storageConfig));
                st.execute();
            } catch (JsonProcessingException e) {
                var mes = "Cannot dump storage data";
                LOG.error(mes + ": {}", e.getMessage());
                throw new RuntimeException(mes, e);
            }
        });
    }

    @Override
    public boolean exists(String userId, String execId) throws SQLException {
        return DbOperation.execute(null, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_SELECT_EXECUTION)) {
                st.setString(1, userId);
                st.setString(2, execId);
                return st.executeQuery().next();
            }
        });
    }

    @Override
    public void setKafkaTopicDesc(String execId, KafkaTopicDesc topicDesc, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Update execution data: { execId: {}, newKafkaDesc: {} }", execId, String.valueOf(topicDesc));
        DbOperation.execute(transaction, storage, con -> {
            try (var stmt = con.prepareStatement(QUERY_UPDATE_KAFKA_TOPIC)) {
                stmt.setString(1, objectMapper.writeValueAsString(topicDesc));
                stmt.setString(2, execId);
                var res = stmt.executeUpdate();

                if (res != 1) {
                    throw new RuntimeException("Expected 1 updated topic desc, but updates "
                        + res + " , execution_id: " + execId);
                }
            } catch (JsonProcessingException e) {
                var mes = "Cannot dump kafka topic desc";
                LOG.error(mes + ": {}", e.getMessage());
                throw new RuntimeException(mes, e);
            }
        });
    }

    @Override
    public void updateAllocatorSession(String execId, String allocSessionId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Update execution data: { execId: {}, newAllocatorSessionId: {} }", execId, allocSessionId);
        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_UPDATE_ALLOCATOR_SESSION)) {
                st.setString(1, allocSessionId);
                st.setString(2, execId);
                if (st.executeUpdate() < 1) {
                    LOG.error("Cannot update allocator session id for unknown execution: { execId: {} }", execId);
                    throw new RuntimeException("Execution with id='%s' not found".formatted(execId));
                }
            }
        });
    }

    @Override
    public void setFinishStatus(String execId, Status status, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LOG.debug("Update execution data: { execId: {}, finishStatus: {} }", execId, status.toString());

        DbOperation.execute(transaction, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_SELECT_FOR_UPDATE_FINISH_DATA,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
            {
                st.setString(1, execId);
                var rs = st.executeQuery();

                if (rs.next()) {
                    if (rs.getTimestamp("finished_at") != null) {
                        LOG.error("Execution already finished: { execId: {}, finishedAt: {}, statusDesc: {}, " +
                                "statusCode: {} }", execId, rs.getTimestamp("finished_at"),
                            rs.getString("finished_with_error"), rs.getInt("finished_error_code"));
                        throw new RuntimeException("Execution with id='%s' already finished".formatted(execId));
                    }

                    rs.updateTimestamp("finished_at", Timestamp.from(Instant.now()));
                    rs.updateString("finished_with_error", status.getDescription());
                    rs.updateInt("finished_error_code", status.getCode().value());
                    rs.updateRow();
                } else {
                    LOG.error("Cannot finish unknown execution: { execId: {} }", execId);
                    throw new RuntimeException("Execution with id='%s' not found".formatted(execId));
                }
            }
        });
    }

    @Override
    @Nullable
    public KafkaTopicDesc getKafkaTopicDesc(String execId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return DbOperation.execute(transaction, storage, con -> {
            try (var stmt = con.prepareStatement(QUERY_SELECT_KAFKA_TOPIC)) {
                stmt.setString(1, execId);
                var qs = stmt.executeQuery();

                if (qs.next()) {
                    var kafkaJson = qs.getString(1);

                    if (kafkaJson == null) {
                        return null;
                    }

                    return objectMapper.readValue(kafkaJson, KafkaTopicDesc.class);
                } else {
                    return null;
                }

            } catch (JsonProcessingException e) {
                var mes = "Cannot deserialize value of kafka topic";
                LOG.error(mes + ": {}", e.getMessage());
                throw new RuntimeException(mes, e);
            }
        });
    }

    @Override
    public LMST.StorageConfig getStorageConfig(String execId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        LMST.StorageConfig[] credentials = {null};

        DbOperation.execute(transaction, storage, connection -> {
            try (var st = connection.prepareStatement(QUERY_SELECT_STORAGE_CREDENTIALS)) {
                st.setString(1, execId);
                var rs = st.executeQuery();

                if (rs.next()) {
                    var dump = rs.getString("storage_credentials");
                    if (dump == null) {
                        LOG.error("Null storage credentials for execution: { execId : {} }", execId);
                        throw new RuntimeException("Null storage credentials for execution with id='%s'".formatted(
                            execId));
                    }
                    credentials[0] = objectMapper.readValue(dump, LMST.StorageConfig.class);
                } else {
                    LOG.error("Cannot get storage credentials for unknown execution: { execId : {} }", execId);
                    throw new RuntimeException("Execution with id='%s' not found".formatted(execId));
                }
            } catch (JsonProcessingException e) {
                var mes = "Cannot deserialize value of storage credentials";
                LOG.error(mes + ": {}", e.getMessage());
                throw new RuntimeException(mes, e);
            }
        });

        return credentials[0];
    }

    @Override
    public StopExecutionState loadStopExecState(String execId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        StopExecutionState result = new StopExecutionState();

        DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_SELECT_STOP_EXECUTION_DATA)) {
                st.setString(1, execId);
                var rs = st.executeQuery();
                if (rs.next()) {
                    var kafkaJson = rs.getString(1);
                    if (kafkaJson != null) {
                        result.kafkaTopicDesc = objectMapper.readValue(kafkaJson, KafkaTopicDesc.class);
                    }
                    result.allocatorSessionId = rs.getString(2);
                } else {
                    LOG.error("Cannot get stop execution data for unknown execution: { execId : {} }", execId);
                    throw new RuntimeException("Execution with id='%s' not found".formatted(execId));
                }
            } catch (JsonProcessingException jpe) {
                var mes = "Cannot deserialize value of kafka topic";
                LOG.error(mes + ": {}", jpe.getMessage());
                throw new RuntimeException(mes, jpe);
            }
        });
        return result;
    }

    @Override
    public ExecuteGraphData loadExecGraphData(String execId, TransactionHandle transaction) throws SQLException {
        return DbOperation.execute(transaction, storage, connection -> {
            try (PreparedStatement st = connection.prepareStatement(QUERY_SELECT_EXEC_GRAPH_DATA)) {
                st.setString(1, execId);

                var rs = st.executeQuery();
                if (rs.next()) {
                    var kafkaJson = rs.getString(1);
                    if (kafkaJson == null) {
                        LOG.error("Null kafka description for execution: { execId : {} }", execId);
                        throw new RuntimeException("Null kafka description for execution with id='%s'"
                            .formatted(execId));
                    }

                    var storageCfgJson = rs.getString(2);
                    if (storageCfgJson == null) {
                        LOG.error("Null storage credentials for execution: { execId : {} }", execId);
                        throw new RuntimeException("Null storage credentials for execution with id='%s'"
                            .formatted(execId));
                    }

                    var allocatorSessionId = rs.getString(3);
                    if (allocatorSessionId == null) {
                        LOG.error("Null allocator session id for execution: { execId : {} }", execId);
                        throw new RuntimeException("Null allocator session id for execution with id='%s'"
                            .formatted(execId));
                    }

                    return new ExecuteGraphData(
                        objectMapper.readValue(storageCfgJson, LMST.StorageConfig.class),
                        objectMapper.readValue(kafkaJson, KafkaTopicDesc.class),
                        allocatorSessionId
                    );
                } else {
                    LOG.error("Cannot get exec graph data for unknown execution: { execId : {} }", execId);
                    throw new RuntimeException("Execution with id='%s' not found".formatted(execId));
                }
            } catch (JsonProcessingException jpe) {
                var mes = "Cannot deserialize value of kafka topic or storage credentials";
                LOG.error(mes + ": {}", jpe.getMessage());
                throw new RuntimeException(mes, jpe);
            }
        });
    }

    @Override
    @Nullable
    public String getExpiredExecution() throws SQLException {
        String[] res = {null};
        DbOperation.execute(null, storage, con -> {
            try (var st = con.prepareStatement(QUERY_SELECT_ONE_EXPIRED_EXECUTION)) {
                var rs = st.executeQuery();
                if (rs.next()) {
                    res[0] = rs.getString(1);
                }
            }
        });
        return res[0];
    }
}
