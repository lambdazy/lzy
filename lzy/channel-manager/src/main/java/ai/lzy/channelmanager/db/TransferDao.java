package ai.lzy.channelmanager.db;

import ai.lzy.channelmanager.model.Peer;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TransferDao {
    /**
     * Create new transfer
     *
     * @return transfer id
     */
    String create(String fromId, String toId, String channelId, State state, String idempotencyKey, String requestHash,
                  @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Transfer get(String id, String channelId, @Nullable TransactionHandle tx) throws SQLException;

    void markActive(String id, String channelId, String idempotencyKey, @Nullable TransactionHandle tx)
        throws SQLException;

    void markFailed(String id, String channelId, String errorDescription, String idempotencyKey,
                    @Nullable TransactionHandle tx) throws SQLException;

    void markCompleted(String id, String channelId, String idempotencyKey, @Nullable TransactionHandle tx)
        throws SQLException;

    boolean hasPendingOrActiveTransfers(String peerId, String channelId,
                                        @Nullable TransactionHandle tx) throws SQLException;

    List<Transfer> listPending(@Nullable TransactionHandle tx) throws SQLException;

    record Transfer(
        String id,
        Peer from,
        Peer to,
        String channelId,
        State state,
        @Nullable String errorDescription,
        @Nullable String idempotencyKey,
        @Nullable String stateChangeIdk
    ) {}

    enum State {
        PENDING,
        ACTIVE,
        COMPLETED,
        FAILED
    }
}
