package ru.yandex.cloud.ml.platform.lzy.server.local;

import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.server.SessionManager;

@Singleton
public class LocalSessionManager implements SessionManager {
    private static final Logger LOG = LogManager.getLogger(LocalSessionManager.class);
    private final Map<String, Set<UUID>> userToSessions = new ConcurrentHashMap<>();

    @Override
    public void registerSession(String userId, UUID sessionId) {
        if (userToSessions.containsKey(userId)) {
            userToSessions.get(userId).add(sessionId);
        } else {
            HashSet<UUID> sessionIds = new HashSet<>();
            sessionIds.add(sessionId);
            userToSessions.put(userId, sessionIds);
        }
    }

    @Override
    public void deleteSession(String userId, UUID sessionId) {
        if (!userToSessions.containsKey(userId)) {
            LOG.warn("Attempt to delete non-existing session user={}, sessionId={}", userId, sessionId);
            return;
        }
        userToSessions.get(userId).remove(sessionId);
    }

    @Override
    public List<UUID> getSessionIds(String userId) {
        return new ArrayList<>(userToSessions.get(userId));
    }
}
