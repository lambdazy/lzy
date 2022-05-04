package ru.yandex.cloud.ml.platform.lzy.kharon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Constants;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TerminalSessionManager {

    private static final Logger LOG = LogManager.getLogger(TerminalSessionManager.class);

    private final Map<UUID, TerminalSession> sessions = new ConcurrentHashMap<>();

    public TerminalSession createSession(TerminalController terminalController) {
        final TerminalSession terminalSession = new TerminalSession(terminalController);
        sessions.put(terminalSession.sessionId(), terminalSession);
        return terminalSession;
    }

    public TerminalSession getTerminalSessionFromGrpcContext() throws InvalidSessionRequestException {
        final UUID sessionId = UUID.fromString(Constants.SESSION_ID_CTX_KEY.get());
        return safeGetSession(sessionId);
    }

    public TerminalSession getTerminalSessionFromSlotUri(String slotUri) throws InvalidSessionRequestException {
        final UUID sessionIdFromUri = UriResolver.parseSessionIdFromSlotUri(URI.create(slotUri));
        return safeGetSession(sessionIdFromUri);
    }

    private TerminalSession safeGetSession(UUID sessionId) throws InvalidSessionRequestException {
        final TerminalSession session = sessions.get(sessionId);
        if (session == null) {
            LOG.error("Got request with unknown sessionId {}", sessionId);
            throw new InvalidSessionRequestException(
                String.format("Unknown sessionId %s", sessionId)
            );
        }
        if (!session.isAlive()) {
            sessions.remove(sessionId);
            LOG.error("Got request on invalid session with id = {}", sessionId);
            throw new InvalidSessionRequestException("Got request on invalid session with id = " + sessionId);
        }
        return session;
    }
}
