package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.v2.model.Channel;
import ai.lzy.channelmanager.v2.model.Peer.Role;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMD;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Singleton
public class ChannelDaoImpl implements ChannelDao {
    private static final Logger LOG = LogManager.getLogger(ChannelDaoImpl.class);

    private static final String FIELDS =
        "id, owner_id, execution_id, workflow_name, data_scheme_json, storage_producer_uri, storage_consumer_uri";

    private static final String GET_CHANNEL = """
        SELECT %s FROM channels
        WHERE id = ?
        """.formatted(FIELDS);

    private static final String FIND_CHANNELS = """
        SELECT %s FROM channels
        WHERE owner_id = ? AND execution_id = ?
            AND storage_producer_uri IS NOT DISTINCT FROM ?
            AND storage_consumer_uri IS NOT DISTINCT FROM ?
        """.formatted(FIELDS);

    private static final String DELETE_FROM_CHANNELS = """
        DELETE FROM channels CASCADE
        WHERE id = ?
        RETURNING %s
        """.formatted(FIELDS);

    private static final String INSERT_INTO_CHANNELS = """
        INSERT INTO channels (%s)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """.formatted(FIELDS);

    private static final String CHANNEL_FIELDS = """
        channels.id, channels.owner_id, channels.execution_id, channels.workflow_name, channels.data_scheme_json,
        channels.storage_producer_uri, channels.storage_consumer_uri
        """;

    private static final String DROP_ALL = """
        DELETE FROM channels CASCADE
        WHERE execution_id = ?
        """;

    private final ChannelManagerDataSource storage;

    public ChannelDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Channel create(String id, String userId, String executionId, String workflowName,
                   @Nullable LMD.DataScheme dataScheme, @Nullable String storageProducerUri,
                   @Nullable String storageConsumerUri, @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(INSERT_INTO_CHANNELS)) {
                ps.setString(1, id);
                ps.setString(2, userId);
                ps.setString(3, executionId);
                ps.setString(4, workflowName);
                ps.setString(5, dataScheme != null ? JsonFormat.printer().print(dataScheme) : null);
                ps.setString(6, storageProducerUri);
                ps.setString(7, storageConsumerUri);
                ps.execute();

                return new Channel(id, userId, workflowName, executionId, dataScheme, storageProducerUri,
                    storageConsumerUri);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot serialize dataScheme into json: ", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    @Nullable
    public Channel drop(String id, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(DELETE_FROM_CHANNELS)) {
                ps.setString(1, id);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }

                return readChannel(rs);
            }
        });
    }

    @Override
    @Nullable
    public Channel find(String userId, String executionId, @Nullable String storageProducerUri,
                        @Nullable String storageConsumerUri, @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(FIND_CHANNELS)) {
                ps.setString(1, userId);
                ps.setString(2, executionId);
                ps.setString(3, storageProducerUri);
                ps.setString(4, storageConsumerUri);

                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return readChannel(rs);
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    @Nullable
    public Channel get(String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(GET_CHANNEL)) {
                ps.setString(1, channelId);

                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return readChannel(rs);
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public List<ChannelStatus> list(String executionId, @Nullable List<String> channelIdsFilter,
                                    @Nullable TransactionHandle tx) throws SQLException
    {
        List<ChannelStatus> channelStatusList = new ArrayList<>();

        final String cond;

        if (channelIdsFilter == null || channelIdsFilter.isEmpty()) {
            cond = "";
        } else {
            cond = "AND channels.id IN (%s)".formatted(
                String.join(",", Collections.nCopies(channelIdsFilter.size(), "?")));
        }

        String query = """
            SELECT %s, array_agg(array[peers.role, peers.description]) as descriptions
            FROM channels
            LEFT JOIN peers ON channels.id = peers.channel_id
            WHERE channels.execution_id = ? %s
            GROUP BY channels.id, channels.owner_id, channels.workflow_name;
            """.formatted(CHANNEL_FIELDS, cond);

        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setString(1, executionId);

                if (channelIdsFilter != null) {
                    for (int i = 0; i < channelIdsFilter.size(); i++) {
                        ps.setString(i + 2, channelIdsFilter.get(i));
                    }
                }

                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    ChannelStatus channelStatus = readChannelStatus(rs);
                    channelStatusList.add(channelStatus);
                }
            }
        });

        return channelStatusList;
    }

    @Override
    public void dropAll(String executionId, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(DROP_ALL)) {
                ps.setString(1, executionId);
                ps.execute();
            }
        });
    }

    private static ChannelStatus readChannelStatus(ResultSet rs) throws SQLException {
        var channel = readChannel(rs);

        var peers = (String[][]) rs.getArray(8).getArray();
        final List<LC.PeerDescription> producers = new ArrayList<>();
        final List<LC.PeerDescription> consumers = new ArrayList<>();

        for (var peer: peers) {
            if (peer == null || peer[0] == null || peer[1] == null) {  // No peers for this channel
                continue;
            }

            var role = Role.valueOf(peer[0]);

            var builder = LC.PeerDescription.newBuilder();
            try {
                JsonFormat.parser().merge(peer[1], builder);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse peerDesc from json:", e);
                throw new RuntimeException(e);
            }

            switch (role) {
                case CONSUMER -> consumers.add(builder.build());
                case PRODUCER -> producers.add(builder.build());
            }
        }

        return new ChannelStatus(channel, consumers, producers);
    }

    private static Channel readChannel(ResultSet rs) throws SQLException {
        var id = rs.getString(1);
        var ownerId = rs.getString(2);
        var executionId = rs.getString(3);
        var workflowName = rs.getString(4);
        var dataSchemeJson = rs.getString(5);
        final LMD.DataScheme dataScheme;

        if (dataSchemeJson == null) {
            dataScheme = null;
        } else {
            try {
                var dsBuilder = LMD.DataScheme.newBuilder();
                JsonFormat.parser().merge(dataSchemeJson, dsBuilder);
                dataScheme = dsBuilder.build();
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse dataScheme from json", e);
                throw new RuntimeException(e);
            }
        }

        var storageProducerUri = rs.getString(6);
        var storageConsumerUri = rs.getString(7);

        return new Channel(id, ownerId, workflowName, executionId, dataScheme,
            storageProducerUri, storageConsumerUri);
    }
}
