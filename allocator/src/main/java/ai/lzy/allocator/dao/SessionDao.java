package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.allocator.model.Session;
import ai.lzy.model.db.TransactionHandle;

import java.sql.SQLException;
import javax.annotation.Nullable;

public interface SessionDao {
    Session create(String owner, CachePolicy cachePolicy, @Nullable TransactionHandle transaction) throws SQLException;

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;

    void delete(String sessionId, @Nullable TransactionHandle transaction) throws SQLException;
}
