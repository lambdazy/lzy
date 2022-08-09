package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.Session;

import javax.annotation.Nullable;
import java.time.Duration;

public interface SessionDao {
    Session create(String owner, Duration minIdleTimeout);

    @Nullable
    Session get(String sessionId);

    @Nullable
    Session delete(String sessionId);
}
