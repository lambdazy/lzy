package ai.lzy.channelmanager.v2;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LC;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface PeerDao {

    Peer create(String channelId, LC.PeerDescription desc, Peer.Role role, Priority priority, boolean connected,
                TransactionHandle tx) throws SQLException;

    /**
     * Find most prior producer in this channel
     */
    @Nullable
    Peer findPriorProducer(String channelId, TransactionHandle tx) throws SQLException;

    /**
     * Atomic request to get all not connected consumers and mark them as connected
     */
    List<Peer> markConsumersAsConnected(String channelId, TransactionHandle tx) throws SQLException;

    /**
     * Marks peer as less prior
     */
    void decrementPriority(String peerId, TransactionHandle tx) throws SQLException;

    Peer drop(String peerId, TransactionHandle tx) throws SQLException;

    enum Priority {
        PRIMARY(10),
        BACKUP(5),
        NO_USE(-1);
        final int val;

        Priority(int val) {
            this.val = val;
        }
    }
}
