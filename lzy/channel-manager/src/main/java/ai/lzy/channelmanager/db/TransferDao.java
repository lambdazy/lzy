package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.model.Peer;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TransferDao {
    void create(String id, String fromId, String toId, String channelId, State state,
                @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Transfer get(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    void markActive(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    void markFailed(String id, String channelId, String errorDescription,
                    @Nullable TransactionHandle tx) throws SQLException;

    void markCompleted(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    boolean hasPendingOrActiveTransfers(String peerId, String channelId,
                                        @Nullable TransactionHandle tx) throws SQLException;

    List<Transfer> listPending(@Nullable TransactionHandle tx) throws SQLException;

    record Transfer(
        String id,
        Peer from,
        Peer to,
        String channelId,
        State state,
        @Nullable String errorDescription
    ) { }

    enum State {
        PENDING,
        ACTIVE,
        COMPLETED,
        FAILED
    }
}
