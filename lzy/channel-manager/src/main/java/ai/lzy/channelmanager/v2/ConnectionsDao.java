package ai.lzy.channelmanager.v2;

import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;

public interface ConnectionsDao {
    void createPendingConnection(String producerPeerId, String consumerPeerId, TransactionHandle tx)
        throws SQLException;

    void dropPendingConnection(String consumerPeerId, TransactionHandle tx) throws SQLException;
}
