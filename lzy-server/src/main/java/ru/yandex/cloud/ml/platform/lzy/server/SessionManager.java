package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.UUID;
import java.util.stream.Stream;

public interface SessionManager {
    void registerSession(String userId, UUID sessionId);

    void deleteSession(String userId, UUID sessionId);

    Stream<UUID> sessionIds(String userId);
}
