package ru.yandex.cloud.ml.platform.lzy.server;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.stream.Stream;

public interface SessionManager {
    Session registerSession(String userId, UUID sessionId, String bucket);
    void deleteSession(UUID sessionId);

    Stream<Session> sessions(String userId);
    @Nullable
    Session get(UUID sessionId);
    @Nullable
    Session byServant(UUID servantId);

    /** [TODO] move this logic to kharon or even terminal */
    Session userSession(String user);

    interface Session {
        UUID id();
        String owner();
        UUID[] servants();
        String bucket();
    }
}
