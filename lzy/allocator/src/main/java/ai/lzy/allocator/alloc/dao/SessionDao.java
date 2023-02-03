package ai.lzy.allocator.alloc.dao;

import ai.lzy.allocator.model.Session;
import ai.lzy.model.db.TransactionHandle;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface SessionDao {

    void create(Session session, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session delete(String sessionId, String deleteOpId, @Nullable TransactionHandle transaction) throws SQLException;

    void touch(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;

    List<Session> listDeleting(@Nullable TransactionHandle transaction) throws SQLException;

    int countActiveSessions() throws SQLException;
}
