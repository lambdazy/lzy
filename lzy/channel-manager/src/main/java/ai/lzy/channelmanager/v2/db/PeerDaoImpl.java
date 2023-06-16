package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.channelmanager.v2.model.Peer.Role;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LC;
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
import java.util.List;

@Singleton
public class PeerDaoImpl implements PeerDao {
    private static final Logger LOG = LogManager.getLogger(PeerDaoImpl.class);

    private final ChannelManagerDataSource storage;

    private static final String FIELDS = "id, channel_id, \"role\", description, priority, connected";

    private static final String DECREMENT_PRIORITY = """
        UPDATE peers
        SET priority = priority - 1
        WHERE id = ? AND channel_id = ?
        RETURNING priority
        """;

    private static final String DROP_PEER = """
        DELETE FROM peers CASCADE
        WHERE id = ? AND channel_id = ?
        RETURNING %s
        """.formatted(FIELDS);

    private static final String GET_PEER = """
        SELECT %s FROM peers
        WHERE id = ? AND channel_id = ?
        """.formatted(FIELDS);

    private static final String MARK_CONSUMERS_AS_CONNECTED = """
        UPDATE peers
        SET connected = true
        WHERE channel_id = ? AND connected = false AND "role" = 'CONSUMER'
        RETURNING %s
        """.formatted(FIELDS);

    private static final String FIND_PRODUCER = """
        SELECT %s FROM peers
        WHERE channel_id = ? AND "role" = 'PRODUCER' AND priority >= 0
        ORDER BY priority DESC, RANDOM() LIMIT 1
        """.formatted(FIELDS);

    private static final String CREATE_PEER = """
        INSERT INTO peers(%s)
        VALUES (?, ?, ?, ?, ?, ?)
        """.formatted(FIELDS);

    public PeerDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Peer create(String channelId, LC.PeerDescription desc, Role role, Priority priority,
                       boolean connected, @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(CREATE_PEER)) {
                String peerId = desc.getPeerId();
                ps.setString(1, peerId);
                ps.setString(2, channelId);
                ps.setString(3, role.name());

                try {
                    ps.setString(4, JsonFormat.printer().print(desc));
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Cannot serialize peerDesc to json", e);
                    throw new RuntimeException(e);
                }
                ps.setInt(5, priority.val);
                ps.setBoolean(6, connected);

                ps.execute();

                return new Peer(peerId, channelId, role, desc);
            }
        });
    }

    @Nullable
    @Override
    public Peer findProducer(String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(FIND_PRODUCER)) {
                ps.setString(1, channelId);

                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return readPeer(rs);
                } else {
                    return null;
                }
            }
        });
    }


    @Override
    public List<Peer> markConsumersAsConnected(String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(MARK_CONSUMERS_AS_CONNECTED)) {
                ps.setString(1, channelId);

                ResultSet rs = ps.executeQuery();

                final ArrayList<Peer> peers = new ArrayList<>();

                while (rs.next()) {
                    peers.add(readPeer(rs));
                }

                return peers;
            }
        });
    }

    @Override
    public int decrementPriority(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(DECREMENT_PRIORITY)) {
                ps.setString(1, id);
                ps.setString(2, channelId);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return -1;
                }

                return rs.getInt(1);
            }
        });
    }

    @Nullable
    @Override
    public Peer drop(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(DROP_PEER)) {
                ps.setString(1, id);
                ps.setString(2, channelId);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                return readPeer(rs);
            }
        });
    }

    @Nullable
    @Override
    public Peer get(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(GET_PEER)) {
                ps.setString(1, id);
                ps.setString(2, channelId);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                return readPeer(rs);
            }
        });
    }

    public static Peer readPeer(ResultSet rs) throws SQLException {
        return readPeer(rs, 0);
    }

    public static Peer readPeer(ResultSet rs, int offset) throws SQLException {
        var id = rs.getString(offset + 1);
        var channelId = rs.getString(offset + 2);
        var role = Role.valueOf(rs.getString(offset + 3));

        var builder = LC.PeerDescription.newBuilder();

        try {
            JsonFormat.parser().merge(rs.getString(offset + 4), builder);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Cannot parse peerDesc from json", e);
            throw new RuntimeException(e);
        }


        return new Peer(id, channelId, role, builder.build());
    }
}
