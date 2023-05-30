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
    private static final String FIELDS = "id, owner_id, execution_id, workflow_name, data_scheme_json," +
        " storage_producer_uri, storage_consumer_uri";

    private static final String CHANNEL_FIELDS = "channel.id, channel.owner_id, channel.execution_id, " +
        "channel.workflow_name, channel.data_scheme_json, channel.storage_producer_uri, channel.storage_consumer_uri";

    private static final Logger LOG = LogManager.getLogger(ChannelDaoImpl.class);

    private final ChannelManagerDataSource storage;

    public ChannelDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Channel create(String id, String userId, String executionId, String workflowName,
                          LMD.DataScheme dataScheme, String storageProducerUri, String storageConsumerUri,
                          TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO channel (%s)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.formatted(FIELDS)))
            {
                ps.setString(1, id);
                ps.setString(2, userId);
                ps.setString(3, executionId);
                ps.setString(4, workflowName);
                ps.setString(5, dataScheme != null ? JsonFormat.printer().print(dataScheme) : null);
                ps.setString(6, storageProducerUri);
                ps.setString(7, storageConsumerUri);
                ps.execute();

                return new Channel(userId, executionId, id, dataScheme, workflowName, storageProducerUri,
                    storageConsumerUri);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot serialize dataScheme into json: ", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Channel drop(String channelId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                DELETE FROM channel
                WHERE id = ?
                RETURNING %s
                """.formatted(FIELDS)))
            {
                ps.setString(1, channelId);
                ResultSet rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                return getChannel(rs);

            }
        });
    }

    @Nullable
    private static Channel getChannel(ResultSet rs) throws SQLException {
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

        return new Channel(ownerId, executionId, id, dataScheme, workflowName,
            storageProducerUri, storageConsumerUri);
    }


    @Override
    public Channel find(String userId, String executionId, String storageProducerUri, String storageConsumerUri,
                        TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                SELECT %s FROM channel
                WHERE owner_id = ? AND execution_id = ?
                    AND storage_producer_uri IS NOT DISTINCT FROM ?
                    AND storage_consumer_uri IS NOT DISTINCT FROM ?
                """.formatted(FIELDS)))
            {
                ps.setString(1, userId);
                ps.setString(2, executionId);
                ps.setString(3, storageProducerUri);
                ps.setString(4, storageConsumerUri);

                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return getChannel(rs);
                } else {
                    return null;
                }
            }
        });
    }



    @Override
    public Channel get(String channelId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                SELECT %s FROM channel
                WHERE id = ?
                """.formatted(FIELDS)))
            {
                ps.setString(1, channelId);

                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return getChannel(rs);
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public List<ChannelStatus> list(String executionId, List<String> channelIdsFilter, TransactionHandle tx)
        throws SQLException
    {
        List<ChannelStatus> channelStatusList = new ArrayList<>();

        final String cond;

        if (channelIdsFilter == null || channelIdsFilter.isEmpty()) {
            cond = "";
        } else {
            cond = "AND channel.id IN (%s)".formatted(
                String.join(",", Collections.nCopies(channelIdsFilter.size(), "?")));
        }

        String query = """
            SELECT %s, array_agg(peer.role || '@@@' || peer.peer_description) as peer_descriptions
            FROM channel
            LEFT JOIN peer ON channel.id = peer.channel_id
            WHERE channel.execution_id = ? %s
            GROUP BY channel.id, channel.owner_id, channel.workflow_name;
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
                    ChannelStatus channelStatus = getChannelStatus(rs);
                    channelStatusList.add(channelStatus);
                }
            }
        });

        return channelStatusList;
    }

    private ChannelStatus getChannelStatus(ResultSet rs) throws SQLException {
        var channel = getChannel(rs);

        var peers = (String[]) rs.getArray(8).getArray();
        final List<LC.PeerDescription> producers = new ArrayList<>();
        final List<LC.PeerDescription> consumers = new ArrayList<>();

        for (var peer: peers) {
            if (peer == null) {  // No peers for this channel
                continue;
            }

            var parts = peer.split("@@@");
            var role = Role.valueOf(parts[0]);

            var builder = LC.PeerDescription.newBuilder();
            try {
                JsonFormat.parser().merge(parts[1], builder);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Cannot parse peerDesc from json:", e);
                throw new RuntimeException(e);
            }

            if (role.equals(Role.CONSUMER)) {
                consumers.add(builder.build());
            } else {
                producers.add(builder.build());
            }
        }

        return new ChannelStatus(channel, consumers, producers);
    }

    @Override
    public void dropAll(String executionId, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                DELETE FROM channel
                WHERE execution_id = ?
                """))
            {
                ps.setString(1, executionId);
                ps.execute();
            }
        });
    }
}
