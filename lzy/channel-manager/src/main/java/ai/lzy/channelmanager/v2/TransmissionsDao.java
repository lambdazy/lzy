package ai.lzy.channelmanager.v2;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface TransmissionsDao {
    Transmission createNotStartedTransmission(String loaderId, String targetId, TransactionHandle tx)
        throws SQLException;

    @Nullable
    Transmission markTransmissionAsStarted(String loaderId, String targetId, TransactionHandle tx) throws SQLException;

    Transmission createStartedTransmission(String loaderId, String targetId, TransactionHandle tx) throws SQLException;


}
