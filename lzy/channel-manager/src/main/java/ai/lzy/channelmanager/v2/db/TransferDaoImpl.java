package ai.lzy.channelmanager.v2.db;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class TransferDaoImpl implements TransferDao {
    private static final String FIELDS = "id, channel_id, from_id, to_id, state, error_description";

    private static final String CREATE = """
        INSERT INTO transfers (%s)
        VALUES (?, ?, ?, ?, ?, ?)
        """.formatted(FIELDS);

    private static final String JOIN_WITH_PEERS = """
        SELECT
          transfers.id, transfers.channel_id, transfers.state, transfers.error_description,
          from_peer.id, from_peer.channel_id, from_peer.role, from_peer.description,
          to_peer.id, to_peer.channel_id, to_peer.role, to_peer.description
        FROM transfers
          JOIN peers from_peer on from_peer.id = transfers.from_id AND from_peer.channel_id = transfers.channel_id
          JOIN peers to_peer on to_peer.id = transfers.to_id AND to_peer.channel_id = transfers.channel_id
        """;

    private static final String LIST_PENDING = JOIN_WITH_PEERS + "\n WHERE state = 'PENDING'";

    private static final String GET = JOIN_WITH_PEERS + "\n WHERE transfers.id = ? AND transfers.channel_id = ?";

    private static final String SET_STATE = """
        UPDATE transfers
          SET state = ?
          WHERE id = ? AND channel_id = ?
        """;

    private static final String MARK_FAILED = """
        UPDATE transfers
          SET state = ?, error_description = ?
          WHERE id = ? AND channel_id = ?
        """;

    private static final String HAS_PENDING_TRANSFERS = """
        SELECT count(*) FROM transfers
         WHERE (from_id = ? OR to_id = ?) AND channel_id = ? AND (state = 'ACTIVE' OR state = 'PENDING')
        """;

    private final ChannelManagerDataSource storage;

    public TransferDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void create(String id, String fromId, String toId, String channelId,
                       State state, @Nullable TransactionHandle tx) throws SQLException
    {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(CREATE)) {
                ps.setString(1, id);
                ps.setString(2, channelId);
                ps.setString(3, fromId);
                ps.setString(4, toId);
                ps.setString(5, state.name());
                ps.setString(6, null);

                ps.execute();
            }
        });
    }

    @Nullable
    @Override
    public Transfer get(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(GET)) {
                ps.setString(1, id);
                ps.setString(2, channelId);

                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                return readTransfer(rs);
            }
        });
    }

    @Override
    public void markActive(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(SET_STATE)) {
                ps.setString(1, State.ACTIVE.name());
                ps.setString(2, id);
                ps.setString(3, channelId);

                ps.execute();
            }
        });
    }

    @Override
    public void markFailed(String id, String channelId, String errorDescription,
                           @Nullable TransactionHandle tx) throws SQLException
    {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(MARK_FAILED)) {
                ps.setString(1, State.FAILED.name());
                ps.setString(2, errorDescription);
                ps.setString(3, id);
                ps.setString(4, channelId);

                ps.execute();
            }
        });
    }

    @Override
    public void markCompleted(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(SET_STATE)) {
                ps.setString(1, State.COMPLETED.name());
                ps.setString(2, id);
                ps.setString(3, channelId);

                ps.execute();
            }
        });
    }

    @Override
    public boolean hasPendingOrActiveTransfers(String peerId, String channelId,
                                               @Nullable TransactionHandle tx) throws SQLException
    {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(HAS_PENDING_TRANSFERS)) {
                ps.setString(1, peerId);
                ps.setString(2, peerId);
                ps.setString(3, channelId);

                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return false;
                }

                return rs.getInt(1) > 0;
            }
        });
    }

    @Override
    public List<Transfer> listPending(@Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement(LIST_PENDING)) {
                var rs = ps.executeQuery();

                final ArrayList<Transfer> transfers = new ArrayList<>();

                while (rs.next()) {
                    transfers.add(readTransfer(rs));
                }

                return transfers;
            }
        });
    }

    private Transfer readTransfer(ResultSet rs) throws SQLException {
        var id = rs.getString(1);
        var channelId = rs.getString(2);
        var state = State.valueOf(rs.getString(3));
        var errorDesc = rs.getString(4);

        var from = PeerDaoImpl.readPeer(rs, 4);
        var to = PeerDaoImpl.readPeer(rs, 8);

        return new Transfer(id, from, to, channelId, state, errorDesc);
    }
}
