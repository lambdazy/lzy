package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConstant;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc.LzyServerBlockingStub;

public class TerminalSessionManager {

    private static final Logger LOG = LogManager.getLogger(TerminalSessionManager.class);

    private final Map<UUID, TerminalSession> sessions = new ConcurrentHashMap<>();
    private final LzyServerBlockingStub server;
    private final URI kharonServantAddress;

    public TerminalSessionManager(LzyServerBlockingStub server, URI kharonServantAddress) {
        this.server = server;
        this.kharonServantAddress = kharonServantAddress;
    }

    public StreamObserver<TerminalState> createSession(StreamObserver<TerminalCommand> terminalCommandObserver) {
        final TerminalSession terminalSession =
            new TerminalSession(server, terminalCommandObserver, kharonServantAddress);
        sessions.put(terminalSession.sessionId(), terminalSession);
        return terminalSession.terminalStateObserver();
    }

    public TerminalSession getTerminalSessionFromGrpcContext() throws InvalidSessionRequestException {
        final UUID sessionId = UUID.fromString(GrpcConstant.SESSION_ID_CTX_KEY.get());
        return safeGetSession(sessionId);
    }

    public TerminalSession getTerminalSessionFromSlotUri(String slotUri) throws InvalidSessionRequestException {
        final UUID sessionIdFromUri = parseSessionIdFromUri(slotUri);
        return safeGetSession(sessionIdFromUri);
    }

    private UUID parseSessionIdFromUri(String slotUri) {
        final URI uri = URI.create(slotUri);
        for (String queryPart : uri.getQuery().split("\\?")) {
            final int equalPos = queryPart.indexOf('=');
            final String key = queryPart.substring(0, equalPos);
            final String value = queryPart.substring(equalPos + 1);
            if (key.equals(TerminalSession.SESSION_ID_KEY)) {
                return UUID.fromString(value);
            }
        }
        throw new IllegalStateException("Failed to parse sessionId from uri " + slotUri);
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
