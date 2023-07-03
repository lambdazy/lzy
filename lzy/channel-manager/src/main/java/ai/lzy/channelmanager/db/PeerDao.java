package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.model.Peer;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.v1.common.LC;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface PeerDao {

    Peer create(String channelId, LC.PeerDescription desc, Peer.Role role, Priority priority, boolean connected,
                @Nullable TransactionHandle tx) throws SQLException;

    /**
     * Find producer with max priority in this channel
     */
    @Nullable
    Peer findProducer(String channelId, @Nullable TransactionHandle tx) throws SQLException;

    /**
     * Atomic request to get all not connected consumers and mark them as connected
     */
    List<Peer> markConsumersAsConnected(String channelId, @Nullable TransactionHandle tx) throws SQLException;

    /**
     * Marks peer as less prior
     * @return new priority
     */
    int decrementPriority(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Peer drop(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Peer get(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    enum Priority {
        PRIMARY(10),
        BACKUP(5),
        DONT_USE(-1);
        final int val;

        Priority(int val) {
            this.val = val;
        }
    }
}
