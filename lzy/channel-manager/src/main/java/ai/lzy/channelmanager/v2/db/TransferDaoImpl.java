package ai.lzy.channelmanager.v2.db;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.TransactionHandle;
import jakarta.inject.Singleton;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class TransferDaoImpl implements TransferDao {
    private final ChannelManagerDataSource storage;

    public TransferDaoImpl(ChannelManagerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void createPendingTransmission(String loaderId, String targetId,
                                                  TransactionHandle tx) throws SQLException
    {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO pending_transfers (slot_id, peer_id)
                VALUES (?, ?)
                """))
            {
                ps.setString(1, loaderId);
                ps.setString(2, targetId);

                ps.execute();
            }
        });
    }

    @Override
    public void dropPendingTransmission(String loaderId, String targetId, TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                DELETE FROM pending_transfers
                 WHERE slot_id = ? AND peer_id = ?
                """))
            {
                ps.setString(1, loaderId);
                ps.setString(2, targetId);

                ps.execute();
            }
        });
    }

    @Override
    public boolean hasPendingTransfers(String peerId, TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                SELECT count(*) FROM pending_transfers
                 WHERE slot_id = ? OR peer_id = ?
                """))
            {
                ps.setString(1, peerId);
                ps.setString(2, peerId);

                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return false;
                }

                return rs.getInt(1) > 0;
            }
        });
    }

    @Override
    public List<Transmission> listPendingTransmissions(TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, connection -> {
            try (PreparedStatement ps = connection.prepareStatement("""
                SELECT slot.id, slot.channel_id, slot.role, slot.peer_description,
                  target.id, target.channel_id, target.role, target.peer_description
                 FROM pending_transfers
                  JOIN peers slot on slot.id = pending_transfers.slot_id
                  JOIN peers target on target.id = pending_transfers.peer_id
                """))
            {
                var rs = ps.executeQuery();

                final ArrayList<Transmission> transmissions = new ArrayList<>();

                while (rs.next()) {
                    var slot = PeerDaoImpl.getPeer(rs);
                    var peer = PeerDaoImpl.getPeer(rs, 4);

                    transmissions.add(new Transmission(slot, peer));
                }

                return transmissions;
            }
        });
    }
}
