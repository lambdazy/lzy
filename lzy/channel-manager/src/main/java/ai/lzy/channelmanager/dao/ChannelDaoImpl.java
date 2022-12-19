package ai.lzy.channelmanager.dao;

import ai.lzy.channelmanager.model.Channel;
import ai.lzy.channelmanager.model.Connection;
import ai.lzy.channelmanager.model.Endpoint;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.ProtoObjectMapper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.db.exceptions.AlreadyExistsException;
import ai.lzy.model.slot.SlotInstance;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.annotation.Nullable;

@Singleton
public class ChannelDaoImpl implements ChannelDao {

    private static final String QUERY_CREATE_CHANNEL = """
        INSERT INTO channels (channel_id, execution_id, workflow_name, user_id,
            channel_name, channel_spec_json, life_status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?::channel_life_status_type, ?, ?)""";

    private static final String QUERY_REMOVE_CHANNEL = """
        DELETE FROM channels
        WHERE channel_id = ? AND life_status = 'DESTROYING'::channel_life_status_type""";

    private static final String QUERY_CHANNEL_SET_STATUS = """
        UPDATE channels
        SET life_status = ?::channel_life_status_type, updated_at = ?
        WHERE channel_id = ?""";

    private static final String QUERY_CREATE_ENDPOINT = """
        INSERT INTO endpoints (slot_uri, "slot_name", slot_owner, task_id, channel_id,
            direction, slot_spec_json, life_status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?::endpoint_life_status_type, ?, ?)""";

    private static final String QUERY_REMOVE_ENDPOINT = """
        DELETE FROM endpoints
        WHERE slot_uri = ? AND life_status = 'UNBINDING'::endpoint_life_status_type""";

    private static final String QUERY_ENDPOINT_SET_STATUS = """
        UPDATE endpoints
        SET life_status = ?::endpoint_life_status_type, updated_at = ?
        WHERE slot_uri = ?""";

    private static final String QUERY_ENDPOINTS_OF_CHANNEL_SET_STATUS = """
        UPDATE endpoints
        SET life_status = ?::endpoint_life_status_type, updated_at = ?
        WHERE channel_id = ?""";

    private static final String QUERY_CREATE_CONNECTION = """
        INSERT INTO connections (sender_uri, receiver_uri, channel_id, life_status, created_at, updated_at)
        VALUES (?, ?, ?, ?::endpoint_life_status_type, ?, ?)""";

    private static final String QUERY_REMOVE_CONNECTION = """
        DELETE FROM connections
        WHERE sender_uri = ? and receiver_uri = ? AND life_status = 'DISCONNECTING'::connection_life_status_type""";

    private static final String QUERY_CONNECTION_SET_STATUS = """
        UPDATE connections
        SET life_status = ?::connection_life_status_type, updated_at = ?
        WHERE sender_uri = ? and receiver_uri = ?""";

    private final ChannelManagerDataSource storage;
    private final ObjectMapper objectMapper;

    public ChannelDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
        this.objectMapper = new ProtoObjectMapper();
    }

    @Override
    public void insertChannel(String channelId, String executionId, String workflowName, String userId,
                              Channel.Spec channelSpec, @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            final var createdAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_CREATE_CHANNEL)) {
                int index = 0;

                st.setString(++index, channelId);
                st.setString(++index, executionId);
                st.setString(++index, workflowName);
                st.setString(++index, userId);
                st.setString(++index, channelSpec.name());
                st.setString(++index, toJson(channelSpec.toString()));

                st.setString(++index, Channel.LifeStatus.ALIVE.name());
                st.setTimestamp(++index, Timestamp.from(createdAt.truncatedTo(ChronoUnit.MILLIS)));
                st.setTimestamp(++index, Timestamp.from(createdAt.truncatedTo(ChronoUnit.MILLIS)));

                try {
                    st.executeUpdate();
                } catch (SQLException e) {
                    if (e.getMessage().contains("channels_pkey")) {
                        throw new AlreadyExistsException("Channel " + channelSpec.name() + " (id=" + channelId + ")" +
                            "of execution " + executionId + " already exists in database");
                    }
                    throw e;
                }
            }
        });
    }

    @Override
    public void markChannelDestroying(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            final var updatedAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_CHANNEL_SET_STATUS)) {
                int index = 0;

                st.setString(++index, Channel.LifeStatus.DESTROYING.name());
                st.setTimestamp(++index, Timestamp.from(updatedAt.truncatedTo(ChronoUnit.MILLIS)));

                st.setString(++index, channelId);

                st.executeUpdate();
            }
        });
    }

    @Override
    public void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_REMOVE_CHANNEL)) {
                st.setString(1, channelId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void insertBindingEndpoint(SlotInstance slot, Endpoint.SlotOwner owner,
                                      @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            final var createdAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_CREATE_ENDPOINT)) {
                int index = 0;

                st.setString(++index, slot.uri().toString());
                st.setString(++index, slot.name());
                st.setString(++index, owner.name());
                st.setString(++index, slot.taskId());
                st.setString(++index, slot.channelId());
                st.setString(++index, slot.spec().direction().name());
                st.setString(++index, toJson(slot.spec()));

                st.setString(++index, Endpoint.LifeStatus.BINDING.name());
                st.setTimestamp(++index, Timestamp.from(createdAt.truncatedTo(ChronoUnit.MILLIS)));
                st.setTimestamp(++index, Timestamp.from(createdAt.truncatedTo(ChronoUnit.MILLIS)));

                st.executeUpdate();
            }
        });
    }

    @Override
    public void markEndpointBound(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            final var updatedAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_ENDPOINT_SET_STATUS)) {
                int index = 0;

                st.setString(++index, Endpoint.LifeStatus.BOUND.name());
                st.setTimestamp(++index, Timestamp.from(updatedAt.truncatedTo(ChronoUnit.MILLIS)));

                st.setString(++index, endpointUri);

                st.executeUpdate();
            }
        });
    }

    @Override
    public void markEndpointUnbinding(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            final var updatedAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_ENDPOINT_SET_STATUS)) {
                int index = 0;

                st.setString(++index, Endpoint.LifeStatus.UNBINDING.name());
                st.setTimestamp(++index, Timestamp.from(updatedAt.truncatedTo(ChronoUnit.MILLIS)));

                st.setString(++index, endpointUri);

                st.executeUpdate();
            }
        });
    }

    @Override
    public void markAllEndpointsUnbinding(String channelId, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            final var updatedAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_ENDPOINTS_OF_CHANNEL_SET_STATUS)) {
                int index = 0;

                st.setString(++index, Endpoint.LifeStatus.UNBINDING.name());
                st.setTimestamp(++index, Timestamp.from(updatedAt.truncatedTo(ChronoUnit.MILLIS)));

                st.setString(++index, channelId);

                st.executeUpdate();
            }
        });
    }

    @Override
    public void removeEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_REMOVE_ENDPOINT)) {
                st.setString(1, endpointUri);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void insertConnection(String channelId, Connection connection, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        if (connection.status() != Connection.LifeStatus.CONNECTING) {
            throw new RuntimeException("Unexpected status " + connection.status() + " of inserting connection " +
                "(senderUri=" + connection.sender().getUri().toString() + ", " +
                "receiverUri=" + connection.receiver().getUri().toString() + ")");
        }
        DbOperation.execute(transaction, storage, conn -> {
            final var createdAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_CREATE_CONNECTION)) {
                int index = 0;

                st.setString(++index, connection.sender().getUri().toString());
                st.setString(++index, connection.receiver().getUri().toString());
                st.setString(++index, channelId);

                st.setString(++index, Connection.LifeStatus.CONNECTING.name());
                st.setTimestamp(++index, Timestamp.from(createdAt.truncatedTo(ChronoUnit.MILLIS)));
                st.setTimestamp(++index, Timestamp.from(createdAt.truncatedTo(ChronoUnit.MILLIS)));

                st.executeUpdate();
            }
        });
    }

    @Override
    public void markConnectionAlive(String channelId, String senderUri, String receiverUri,
                                    @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            final var updatedAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_CONNECTION_SET_STATUS)) {
                int index = 0;

                st.setString(++index, Connection.LifeStatus.CONNECTED.name());
                st.setTimestamp(++index, Timestamp.from(updatedAt.truncatedTo(ChronoUnit.MILLIS)));

                st.setString(++index, senderUri);
                st.setString(++index, receiverUri);

                st.executeUpdate();
            }
        });
    }

    @Override
    public void markConnectionDisconnecting(String channelId, String senderUri, String receiverUri,
                                            @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            final var updatedAt = Instant.now();
            try (var st = conn.prepareStatement(QUERY_CONNECTION_SET_STATUS)) {
                int index = 0;

                st.setString(++index, Connection.LifeStatus.DISCONNECTING.name());
                st.setTimestamp(++index, Timestamp.from(updatedAt.truncatedTo(ChronoUnit.MILLIS)));

                st.setString(++index, senderUri);
                st.setString(++index, receiverUri);

                st.executeUpdate();
            }
        });
    }

    @Override
    public void removeConnection(String channelId, String senderUri, String receiverUri,
                                 @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, storage, conn -> {
            try (var st = conn.prepareStatement(QUERY_REMOVE_CONNECTION)) {
                st.setString(1, senderUri);
                st.setString(2, receiverUri);
                st.executeUpdate();
            }
        });
    }

    @Nullable
    @Override
    public Channel findChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        return null; // TODO
    }

    @Nullable
    @Override
    public Channel findChannel(String channelId, Channel.LifeStatus lifeStatus, @Nullable TransactionHandle transaction)
        throws SQLException
    {
        return null; // TODO
    }

    @Override
    public List<Channel> listChannels(String executionId, @Nullable TransactionHandle transaction) throws SQLException {
        return null; // TODO
    }

    @Override
    public List<Channel> listChannels(String executionId, Channel.LifeStatus lifeStatus,
                                      @Nullable TransactionHandle transaction) throws SQLException
    {
        return null; // TODO
    }

    @Nullable
    @Override
    public Endpoint findEndpoint(String endpointUri, @Nullable TransactionHandle transaction) throws SQLException {
        return null; // TODO
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T fromJson(String obj, Class<T> type) {
        try {
            return objectMapper.readValue(obj, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
