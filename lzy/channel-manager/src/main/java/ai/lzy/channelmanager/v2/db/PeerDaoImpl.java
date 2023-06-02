package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.channelmanager.v2.model.Peer.Role;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LC;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
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
    private final ChannelManagerDataSource storage;
    private static final String FIELDS = "id, channel_id, \"role\", peer_description, priority, connected";
    private static final Logger LOG = LogManager.getLogger(PeerDaoImpl.class);

    public PeerDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public Peer create(String channelId, LC.PeerDescription desc, Role role, Priority priority,
                       boolean connected, TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO peers(%s)
                VALUES (?, ?, ?, ?, ?, ?)
                """.formatted(FIELDS)))
            {
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

    @Override
    public Peer findProducer(String channelId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                SELECT %s FROM peers
                WHERE channel_id = ? AND "role" = 'PRODUCER' AND priority >= 0
                ORDER BY priority DESC, RANDOM() LIMIT 1
                """.formatted(FIELDS)))
            {
                ps.setString(1, channelId);

                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return getPeer(rs);
                } else {
                    return null;
                }
            }
        });
    }


    @Override
    public List<Peer> markConsumersAsConnected(String channelId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                UPDATE peers
                SET connected = true
                WHERE channel_id = ? AND connected = false AND "role" = 'CONSUMER'
                RETURNING %s
                """.formatted(FIELDS)))
            {
                ps.setString(1, channelId);

                ResultSet rs = ps.executeQuery();

                final ArrayList<Peer> peers = new ArrayList<>();

                while (rs.next()) {
                    peers.add(getPeer(rs));
                }

                return peers;
            }
        });
    }

    @Override
    public int decrementPriority(String peerId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                UPDATE peers
                SET priority = priority - 1
                WHERE id = ?
                RETURNING priority
                """))
            {
                ps.setString(1, peerId);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return -1;
                }

                return rs.getInt(1);
            }
        });
    }

    @Override
    public Peer drop(String peerId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                DELETE FROM peers CASCADE
                WHERE id = ?
                RETURNING %s
                """.formatted(FIELDS)))
            {
                ps.setString(1, peerId);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                return getPeer(rs);
            }
        });
    }

    @Override
    public Peer get(String peerId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                SELECT %s FROM peers
                WHERE id = ?
                """.formatted(FIELDS)))
            {
                ps.setString(1, peerId);

                ResultSet rs = ps.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                return getPeer(rs);
            }
        });
    }

    public static Peer getPeer(ResultSet rs) throws SQLException {
        return getPeer(rs, 0);
    }

    public static Peer getPeer(ResultSet rs, int offset) throws SQLException {
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
