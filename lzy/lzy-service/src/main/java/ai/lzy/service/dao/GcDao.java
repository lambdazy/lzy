package ai.lzy.service.dao;

import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.sql.Timestamp;

public interface GcDao {
    boolean updateGC(String id, Timestamp now, Timestamp validUntil, @Nullable TransactionHandle transaction)
        throws SQLException;
    boolean markGCValid(String id, Timestamp now, Timestamp validUntil, @Nullable TransactionHandle transaction)
        throws SQLException;
}
