package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.sql.Timestamp;
import javax.annotation.Nullable;

public interface GcDao {
    void updateGC(String id, Timestamp now, Timestamp validUntil, @Nullable TransactionHandle transaction)
        throws SQLException;
}
