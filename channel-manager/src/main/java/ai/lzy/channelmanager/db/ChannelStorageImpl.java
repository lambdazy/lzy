package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.channel.Channel;
import ai.lzy.channelmanager.channel.ChannelImpl;
import ai.lzy.channelmanager.channel.Endpoint;
import ai.lzy.channelmanager.channel.SlotEndpoint;
import ai.lzy.channelmanager.control.DirectChannelController;
import ai.lzy.channelmanager.control.SnapshotChannelController;
import ai.lzy.model.deprecated.GrpcConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.channelmanager.channel.ChannelSpec;
import ai.lzy.channelmanager.channel.DirectChannelSpec;
import ai.lzy.channelmanager.channel.SnapshotChannelSpec;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.ProtoObjectMapper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.v1.channel.LCM;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyZygote;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ChannelStorageImpl implements ChannelStorage {

    private static final Logger LOG = LogManager.getLogger(ChannelStorageImpl.class);

    private final ChannelManagerDataSource dataSource;
    private final ObjectMapper objectMapper;

    @Inject
    public ChannelStorageImpl(ChannelManagerDataSource dataSource) {
        this.dataSource = dataSource;
        this.objectMapper = new ProtoObjectMapper();
    }

    @Override
    public void insertChannel(String channelId, String userId, String workflowId, String channelName,
                              LCM.ChannelSpec.TypeCase channelType, ChannelSpec channelSpec,
                              @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO channels(
                    channel_id, user_id, workflow_id, channel_name,
                    channel_type, channel_spec, created_at, channel_life_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)
            ) {
                String channelSpecJson = objectMapper.writeValueAsString(channelSpec);
                int index = 0;
                st.setString(++index, channelId);
                st.setString(++index, userId);
                st.setString(++index, workflowId);
                st.setString(++index, channelName);
                st.setString(++index, channelType.name());
                st.setString(++index, channelSpecJson);
                st.setTimestamp(++index, Timestamp.from(Instant.now()));
                st.setString(++index, ChannelLifeStatus.ALIVE.name());
                st.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
    }

    @Override
    public void removeChannel(String channelId, @Nullable TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "DELETE FROM channels WHERE channel_id = ?"
            )) {
                int index = 0;
                st.setString(++index, channelId);
                st.execute();
            }
        });
    }

    @Override
    public void insertEndpoint(Endpoint endpoint, TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO channel_endpoints(
                    channel_id, "slot_name", slot_uri, direction, task_id, slot_spec
                ) VALUES (?, ?, ?, ?, ?, ?)
                """)
            ) {
                String slotSpecJson = objectMapper.writeValueAsString(ProtoConverter.toProto(endpoint.slotSpec()));
                int index = 0;
                st.setString(++index, endpoint.slotInstance().channelId());
                st.setString(++index, endpoint.slotSpec().name());
                st.setString(++index, endpoint.uri().toString());
                st.setString(++index, endpoint.slotSpec().direction().name());
                st.setString(++index, endpoint.taskId());
                st.setString(++index, slotSpecJson);
                st.executeUpdate();
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
    }

    @Override
    public void insertEndpointConnections(String channelId, Map<Endpoint, Endpoint> edges,
                                          TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                INSERT INTO endpoint_connections (
                    channel_id, sender_uri, receiver_uri
                ) SELECT ?, slot_uri.sender, slot_uri.receiver
                FROM unnest(?, ?) AS slot_uri(sender, receiver)
                """)
            ) {
                final List<String> senderUris = new ArrayList<>();
                final List<String> receiverUris = new ArrayList<>();
                edges.forEach((sender, receiver) -> {
                    senderUris.add(sender.uri().toString());
                    receiverUris.add(receiver.uri().toString());
                });

                int index = 0;
                st.setString(++index, channelId);
                st.setArray(++index, sqlConnection.createArrayOf("text", senderUris.toArray()));
                st.setArray(++index, sqlConnection.createArrayOf("text", receiverUris.toArray()));
                st.executeUpdate();
            }
        });
    }

    @Override
    public void removeEndpointWithConnections(Endpoint endpoint, TransactionHandle transaction) throws SQLException {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "DELETE FROM channel_endpoints WHERE slot_uri = ?"
            )) {
                int index = 0;
                st.setString(++index, endpoint.uri().toString());
                st.execute();
            }
        });
    }

    @Override
    public void setChannelLifeStatus(String channelId, ChannelLifeStatus lifeStatus,
                                     @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "UPDATE channels SET channel_life_status = ? WHERE channel_id = ?"
            )) {
                int index = 0;
                st.setString(++index, lifeStatus.name());
                st.setString(++index, channelId);
                st.executeUpdate();
            }
        });
    }

    @Override
    public void setChannelLifeStatus(String userId, String workflowId, ChannelLifeStatus lifeStatus,
                                     @Nullable TransactionHandle transaction) throws SQLException
    {
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement(
                "UPDATE channels SET channel_life_status = ? WHERE user_id = ? AND workflow_id = ?"
            )) {
                int index = 0;
                st.setString(++index, lifeStatus.name());
                st.setString(++index, userId);
                st.setString(++index, workflowId);
                st.executeUpdate();
            }
        });
    }

    @Nullable
    @Override
    public Channel findChannel(String channelId, ChannelLifeStatus lifeStatus,
                               @Nullable TransactionHandle transaction) throws SQLException
    {
        final AtomicReference<Channel> channel = new AtomicReference<>();
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    ch.channel_id as channel_id,
                    ch.user_id as user_id,
                    ch.workflow_id as workflow_id,
                    ch.channel_name as channel_name,
                    ch.channel_type as channel_type,
                    ch.channel_spec as channel_spec,
                    ch.created_at as created_at,
                    ch.channel_life_status as channel_life_status,
                    e.slot_uri as slot_uri,
                    e.direction as direction,
                    e.task_id as task_id,
                    e.slot_spec as slot_spec,
                    c.receiver_uri as connected_slot_uri
                FROM channels ch
                LEFT JOIN channel_endpoints e ON ch.channel_id = e.channel_id
                LEFT JOIN endpoint_connections c ON e.channel_id = c.channel_id AND e.slot_uri = c.sender_uri
                WHERE ch.channel_id = ? AND ch.channel_life_status = ?
                """)
            ) {
                int index = 0;
                st.setString(++index, channelId);
                st.setString(++index, lifeStatus.name());
                Stream<Channel> channels = parseChannels(st.executeQuery());
                channel.set(channels.findFirst().orElse(null));
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
        return channel.get();
    }

    @Override
    public List<Channel> listChannels(String userId, String workflowId, ChannelLifeStatus lifeStatus,
                                      @Nullable TransactionHandle transaction) throws SQLException
    {
        final List<Channel> channels = new ArrayList<>();
        DbOperation.execute(transaction, dataSource, sqlConnection -> {
            try (final PreparedStatement st = sqlConnection.prepareStatement("""
                SELECT
                    ch.channel_id as channel_id,
                    ch.user_id as user_id,
                    ch.workflow_id as workflow_id,
                    ch.channel_name as channel_name,
                    ch.channel_type as channel_type,
                    ch.channel_spec as channel_spec,
                    ch.created_at as created_at,
                    ch.channel_life_status as channel_life_status,
                    e.slot_uri as slot_uri,
                    e.direction as direction,
                    e.task_id as task_id,
                    e.slot_spec as slot_spec,
                    c.receiver_uri as connected_slot_uri
                FROM channels ch
                LEFT JOIN channel_endpoints e ON ch.channel_id = e.channel_id
                LEFT JOIN endpoint_connections c ON e.channel_id = c.channel_id AND e.slot_uri = c.sender_uri
                WHERE ch.user_id = ? AND ch.workflow_id = ? AND ch.channel_life_status = ?
                """)
            ) {
                int index = 0;
                st.setString(++index, userId);
                st.setString(++index, workflowId);
                st.setString(++index, lifeStatus.name());
                channels.addAll(parseChannels(st.executeQuery()).toList());
            } catch (JsonProcessingException e) {
                throw new SQLException(e);
            }
        });
        return channels;
    }

    private Stream<Channel> parseChannels(ResultSet rs) throws SQLException, JsonProcessingException {
        Map<String, Channel.Builder> chanelBuildersById = new HashMap<>();
        Map<String, HashSet<String>> slotsUriByChannelId = new HashMap<>();
        while (rs.next()) {
            final String channelId = rs.getString("channel_id");
            if (!chanelBuildersById.containsKey(channelId)) {
                chanelBuildersById.put(channelId, ChannelImpl.newBuilder()
                    .setId(channelId)
                    .setWorkflowId(rs.getString("workflow_id")));
                slotsUriByChannelId.put(channelId, new HashSet<>());
                var channelType = LCM.ChannelSpec.TypeCase.valueOf(rs.getString("channel_type"));
                switch (channelType) {
                    case DIRECT -> {
                        chanelBuildersById.get(channelId).setSpec(objectMapper.readValue(
                            rs.getString("channel_spec"), DirectChannelSpec.class
                        ));
                        chanelBuildersById.get(channelId).setController(new DirectChannelController());
                    }
                    case SNAPSHOT -> {
                        SnapshotChannelSpec spec = objectMapper.readValue(
                            rs.getString("channel_spec"), SnapshotChannelSpec.class
                        );
                        chanelBuildersById.get(channelId).setSpec(spec);
                        chanelBuildersById.get(channelId).setController(new SnapshotChannelController(
                            spec.entryId(), spec.snapshotId(), rs.getString("user_id"), spec.whiteboardAddress()
                        ));
                    }
                    default -> {
                        final String errorMessage = String.format(
                            "Unexpected chanel type \"%s\", only snapshot and direct channels are supported",
                            channelType
                        );
                        LOG.error(errorMessage);
                        throw new NotImplementedException(errorMessage);
                    }
                }
            }
            final String slotUri = rs.getString("slot_uri");
            final String connectedSlotUri = rs.getString("connected_slot_uri");
            if (slotUri != null && !slotsUriByChannelId.get(channelId).contains(slotUri)) {
                slotsUriByChannelId.get(channelId).add(slotUri);
                var slot = objectMapper.readValue(rs.getString("slot_spec"), LMS.Slot.class);
                var endpoint = SlotEndpoint.getInstance(new SlotInstance(
                    ProtoConverter.fromProto(slot),
                    rs.getString("task_id"),
                    channelId,
                    URI.create(slotUri)
                ));
                switch (endpoint.slotSpec().direction()) {
                    case OUTPUT -> chanelBuildersById.get(channelId).addSender(endpoint);
                    case INPUT -> chanelBuildersById.get(channelId).addReceiver(endpoint);
                }
            }
            if (connectedSlotUri != null) {
                chanelBuildersById.get(channelId).addEdge(slotUri, connectedSlotUri);
            }
        }
        return chanelBuildersById.values().stream().map(Channel.Builder::build);
    }
}
