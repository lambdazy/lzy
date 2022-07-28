package ai.lzy.kharon;

import ai.lzy.v1.LzyServerGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TerminalSessionManager {

    private static final Logger LOG = LogManager.getLogger(TerminalSessionManager.class);

    private final Map<String, TerminalSession> sessions = new ConcurrentHashMap<>();

    public TerminalSession createSession(
        String sessionId,
        TerminalController terminalController,
        LzyServerGrpc.LzyServerBlockingStub server
    ) {
        final TerminalSession terminalSession = new TerminalSession(sessionId, terminalController, server);
        sessions.put(terminalSession.sessionId(), terminalSession);
        return terminalSession;
    }

    public TerminalSession get(String sessionId) throws InvalidSessionRequestException {
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
