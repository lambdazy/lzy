package ai.lzy.channelmanager.v2;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface TransmissionsDao {
    Transmission createPendingTransmission(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    @Nullable
    Transmission dropPendingTransmission(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    List<Transmission> listPendingTransmissions(TransactionHandle tx) throws SQLException;

    record Transmission(
        Peer loader,
        Peer target
    ) { }
}
