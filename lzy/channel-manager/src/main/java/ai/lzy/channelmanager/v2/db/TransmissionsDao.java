package ai.lzy.channelmanager.v2.db;

import ai.lzy.channelmanager.v2.model.Peer;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.util.List;

public interface TransmissionsDao {
    void createPendingTransmission(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    void dropPendingTransmission(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    boolean hasPendingTransfers(String peerId, TransactionHandle tx) throws SQLException;

    List<Transmission> listPendingTransmissions(TransactionHandle tx) throws SQLException;

    record Transmission(
        Peer loader,
        Peer target
    ) { }
}
