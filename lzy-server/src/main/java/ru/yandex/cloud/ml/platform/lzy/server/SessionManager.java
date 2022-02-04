package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.List;
import java.util.UUID;

public interface SessionManager {
    void registerSession(String userId, UUID sessionId);
    void deleteSession(String userId, UUID sessionId);
    List<UUID> getSessionIds(String userId);
}
