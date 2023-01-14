package ai.lzy.allocator.alloc.dao;

import ai.lzy.allocator.model.Session;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;

public interface SessionDao {

    void create(Session session, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session delete(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;
}
