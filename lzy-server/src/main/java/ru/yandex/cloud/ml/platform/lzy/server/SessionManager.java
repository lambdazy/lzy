package ru.yandex.cloud.ml.platform.lzy.server;

import java.util.UUID;
import java.util.stream.Stream;

public interface SessionManager {
    void registerSession(String userId, UUID sessionId, String bucket);
    void deleteSession(UUID sessionId);

    Session get(UUID sessionId);
    Stream<Session> sessions(String userId);
    Session byServant(String servantId);

    interface Session {
        UUID id();
        String owner();
        UUID[] servants();
        String bucket();
    }
}
