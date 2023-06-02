package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.util.List;

public interface TransferDao {
    void createPendingTransfer(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    void dropPendingTransfer(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    boolean hasPendingTransfers(String peerId, TransactionHandle tx) throws SQLException;

    List<Transfer> listPendingTransmissions(TransactionHandle tx) throws SQLException;

    record Transfer(
        Peer from,
        Peer to
    ) { }
}
