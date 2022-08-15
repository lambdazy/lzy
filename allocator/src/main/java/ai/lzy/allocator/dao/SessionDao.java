package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.CachePolicy;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.allocator.model.Session;

import javax.annotation.Nullable;

public interface SessionDao {
    Session create(String owner, CachePolicy cachePolicy, @Nullable TransactionHandle transaction);

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction);

    void delete(String sessionId, @Nullable TransactionHandle transaction);
}
