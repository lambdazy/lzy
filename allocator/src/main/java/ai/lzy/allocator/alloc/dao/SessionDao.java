package ai.lzy.allocator.alloc.dao;

import ai.lzy.allocator.model.Session;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import javax.annotation.Nullable;

public interface SessionDao {

    void create(Session session, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;

    boolean delete(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;
}
