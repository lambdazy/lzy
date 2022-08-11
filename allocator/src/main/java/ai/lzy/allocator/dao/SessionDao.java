package ai.lzy.allocator.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.allocator.model.Session;

import javax.annotation.Nullable;
import java.time.Duration;

public interface SessionDao {
    Session create(String owner, Duration minIdleTimeout, @Nullable TransactionHandle transaction);

    @Nullable
    Session get(String sessionId, @Nullable TransactionHandle transaction);

    @Nullable
    Session delete(String sessionId, @Nullable TransactionHandle transaction);
}
