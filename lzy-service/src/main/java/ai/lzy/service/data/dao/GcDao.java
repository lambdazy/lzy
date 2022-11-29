package ai.lzy.service.data.dao;

import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import java.sql.Timestamp;
import javax.annotation.Nullable;

public interface GcDao {
    void insertNewGcSession(@Nullable TransactionHandle transaction, String id) throws SQLException;

    void updateStatus(@Nullable TransactionHandle transaction, String id) throws SQLException;

    @Nullable
    Timestamp getLastUpdated() throws SQLException;
}
