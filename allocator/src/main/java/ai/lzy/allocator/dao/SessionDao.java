package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.Session;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import javax.annotation.Nullable;

public interface SessionDao {

    void create(Session session, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;

    void delete(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;
}
