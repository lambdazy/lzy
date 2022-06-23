package ai.lzy.kharon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.Constants;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TerminalSessionManager {

    private static final Logger LOG = LogManager.getLogger(TerminalSessionManager.class);

    private final Map<String, TerminalSession> sessions = new ConcurrentHashMap<>();

    public TerminalSession createSession(
        String sessionId,
        TerminalController terminalController,
        ServerControllerFactory serverControllerFactory
    ) {
        final TerminalSession terminalSession =
                new TerminalSession(sessionId, terminalController, serverControllerFactory);
        sessions.put(terminalSession.sessionId(), terminalSession);
        return terminalSession;
    }

    public TerminalSession getSessionFromGrpcContext() throws InvalidSessionRequestException {
        final String sessionId = Constants.SESSION_ID_CTX_KEY.get();
        return safeGetSession(sessionId);
    }

    public TerminalSession getSessionFromSlotUri(String slotUri) throws InvalidSessionRequestException {
        final String sessionIdFromUri = UriResolver.parseSessionIdFromSlotUri(URI.create(slotUri));
        return safeGetSession(sessionIdFromUri);
    }

    private TerminalSession safeGetSession(String sessionId) throws InvalidSessionRequestException {
        final TerminalSession session = sessions.get(sessionId);
        if (session == null) {
            LOG.error("Got request with unknown sessionId {}", sessionId);
            throw new InvalidSessionRequestException(
                String.format("Unknown sessionId %s", sessionId)
            );
        }
        final TerminalSessionState state = session.state();
        if (state == TerminalSessionState.ERRORED || state == TerminalSessionState.COMPLETED) {
            deleteSession(sessionId);
            LOG.error("Got request on invalid session with id = {}", sessionId);
            throw new InvalidSessionRequestException("Got request on invalid session with id = " + sessionId);
        }
        return session;
    }

    public void deleteSession(String sessionId) {
        LOG.info("Deleting session with id={}", sessionId);
        sessions.remove(sessionId);
    }
}
